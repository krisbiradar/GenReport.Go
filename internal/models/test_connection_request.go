package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type ProviderInput struct {
	IntValue    *int
	StringValue string
}

func (p *ProviderInput) UnmarshalJSON(data []byte) error {
	var intVal int
	if err := json.Unmarshal(data, &intVal); err == nil {
		p.IntValue = &intVal
		p.StringValue = ""
		return nil
	}

	var strVal string
	if err := json.Unmarshal(data, &strVal); err == nil {
		p.IntValue = nil
		p.StringValue = strings.TrimSpace(strVal)
		return nil
	}

	return fmt.Errorf("provider must be a number or string")
}

func (p ProviderInput) IsEmpty() bool {
	return p.IntValue == nil && strings.TrimSpace(p.StringValue) == ""
}

type TestConnectionRequest struct {
	Name             string       `json:"name"`
	DatabaseType     string       `json:"databaseType"`
	Provider         ProviderInput `json:"provider"`
	HostName         string       `json:"hostName"`
	Port             int          `json:"port"`
	UserName         string       `json:"userName"`
	DatabaseName     string       `json:"databaseName"`
	Password         string       `json:"password"`
	ConnectionString string       `json:"connectionString"`
	Description      string       `json:"description"`
}

func (r TestConnectionRequest) HasConnectionString() bool {
	return strings.TrimSpace(r.ConnectionString) != ""
}

func (r TestConnectionRequest) Validate() error {
	if strings.TrimSpace(r.DatabaseType) == "" && r.Provider.IsEmpty() {
		return errors.New("databaseType or provider is required")
	}

	if !r.HasConnectionString() {
		if strings.TrimSpace(r.HostName) == "" {
			return errors.New("hostName is required when connectionString is not provided")
		}
	}

	return nil
}
