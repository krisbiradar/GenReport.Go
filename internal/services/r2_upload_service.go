package services

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"strings"

	"genreport/internal/config"
	"genreport/internal/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

// R2UploadService uploads files to Cloudflare R2 using the S3-compatible API.
type R2UploadService struct {
	client    *s3.Client
	bucket    string
	publicURL string // e.g. https://<custom-domain> or https://pub-<hash>.r2.dev
	logger    zerolog.Logger
}

func NewR2UploadService(cfg config.R2Config, logger zerolog.Logger) (*R2UploadService, error) {
	r2Endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", cfg.AccountID)

	awsCfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		),
		awscfg.WithRegion("auto"),
	)
	if err != nil {
		return nil, fmt.Errorf("r2: failed to load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(r2Endpoint)
		// R2 uses path-style addressing
		o.UsePathStyle = true
	})

	return &R2UploadService{
		client:    client,
		bucket:    cfg.Bucket,
		publicURL: strings.TrimRight(cfg.PublicURL, "/"),
		logger:    logger,
	}, nil
}

// Upload decodes the base64 content, puts the object into R2, and returns the
// public URL. Returns an empty string if the upload fails.
func (s *R2UploadService) Upload(ctx context.Context, req models.UploadFileRequest) string {
	raw, err := base64.StdEncoding.DecodeString(req.Content)
	if err != nil {
		// Try URL-safe variant before giving up
		raw, err = base64.URLEncoding.DecodeString(req.Content)
		if err != nil {
			s.logger.Error().
				Str("fileName", req.FileName).
				Err(err).
				Msg("r2: failed to decode base64 content")
			return ""
		}
	}

	key := sanitizeKey(req.FileName)

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(raw),
		ContentType:   aws.String(req.MimeType),
		ContentLength: aws.Int64(int64(len(raw))),
	})
	if err != nil {
		s.logger.Error().
			Str("fileName", req.FileName).
			Str("key", key).
			Err(err).
			Msg("r2: PutObject failed")
		return ""
	}

	url := fmt.Sprintf("%s/%s", s.publicURL, key)
	s.logger.Info().
		Str("key", key).
		Str("url", url).
		Msg("r2: file uploaded successfully")

	return url
}

// sanitizeKey cleans the path and strips leading slashes so the key is always relative.
func sanitizeKey(name string) string {
	cleaned := path.Clean("/" + strings.TrimSpace(name))
	return strings.TrimLeft(cleaned, "/")
}

