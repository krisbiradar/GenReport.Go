package services

import (
	"fmt"

	"genreport/internal/config"
	"genreport/internal/models"

	"github.com/rs/zerolog"
	"github.com/wneessen/go-mail"
	"gorm.io/gorm"
)

type EmailService struct {
	cfg    config.SMTPConfig
	db     *gorm.DB
	logger zerolog.Logger
}

func NewEmailService(cfg config.SMTPConfig, db *gorm.DB, logger zerolog.Logger) *EmailService {
	return &EmailService{
		cfg:    cfg,
		db:     db,
		logger: logger,
	}
}

func (s *EmailService) SendJobFailureAlert(jobName string, err error) {
	if s.cfg.Host == "" {
		s.logger.Warn().Msg("SMTP is not configured, skipping failure email")
		return
	}

	if s.db == nil {
		s.logger.Warn().Msg("Database connection is not available, skipping failure email")
		return
	}

	// Fetch admin email dynamically: first user with role_id == 1
	var adminUser models.User
	if errDB := s.db.Where("role_id = ?", 1).First(&adminUser).Error; errDB != nil {
		s.logger.Error().Err(errDB).Msg("Failed to fetch admin user for email notification")
		return
	}

	if adminUser.Email == "" {
		s.logger.Warn().Msg("Admin user found but has no email configured")
		return
	}

	// Prepare the email
	m := mail.NewMsg()
	if errMail := m.From(s.cfg.From); errMail != nil {
		s.logger.Error().Err(errMail).Msg("Failed to set FROM address")
		return
	}
	if errMail := m.To(adminUser.Email); errMail != nil {
		s.logger.Error().Err(errMail).Msg("Failed to set TO address")
		return
	}

	m.Subject(fmt.Sprintf("[ALERT] Background Job Failed: %s", jobName))
	m.SetBodyString(mail.TypeTextPlain, fmt.Sprintf(
		"The background job '%s' has failed and has been disabled until the server restarts.\n\nError details:\n%v",
		jobName, err,
	))

	client, errClient := mail.NewClient(
		s.cfg.Host,
		mail.WithPort(s.cfg.Port),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(s.cfg.Username),
		mail.WithPassword(s.cfg.Password),
	)
	if errClient != nil {
		s.logger.Error().Err(errClient).Msg("Failed to create SMTP client")
		return
	}

	if errSend := client.DialAndSend(m); errSend != nil {
		s.logger.Error().Err(errSend).Str("job", jobName).Msg("Failed to send job failure email — continuing")
		return
	}

	s.logger.Info().Str("to", adminUser.Email).Str("job", jobName).Msg("Job failure alert email sent successfully")
}
