package models

import (
	"errors"
	"strings"
)

// UploadFileRequest is the JSON body accepted by POST /storage/upload.
type UploadFileRequest struct {
	FileName string `json:"fileName"`
	Content  string `json:"content"`  // base64-encoded file bytes
	MimeType string `json:"mimeType"`
}

func (r UploadFileRequest) Validate() error {
	if strings.TrimSpace(r.FileName) == "" {
		return errors.New("fileName is required")
	}
	if strings.TrimSpace(r.Content) == "" {
		return errors.New("content is required")
	}
	if strings.TrimSpace(r.MimeType) == "" {
		return errors.New("mimeType is required")
	}
	return nil
}

// UploadFileResponse is returned by POST /storage/upload.
// On success, URL holds the public link to the uploaded object.
// On failure, URL is an empty string.
type UploadFileResponse struct {
	URL string `json:"url"`
}
