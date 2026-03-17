package db

import (
	"context"
	"errors"

	"genreport/internal/models"
)

type OracleProvider struct {
}

func NewOracleProvider() *OracleProvider {
	return &OracleProvider{}
}

func (p *OracleProvider) TestConnection(ctx context.Context, req models.TestConnectionRequest) error {
	return errors.New("Oracle provider is not implemented")
}
