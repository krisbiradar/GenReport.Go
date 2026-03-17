package db

import "genreport/internal/config"

type ProviderFactory struct {
	providers map[ProviderKind]DBProvider
}

func NewProviderFactory(cfg config.Config) *ProviderFactory {
	return &ProviderFactory{
		providers: map[ProviderKind]DBProvider{
			ProviderPostgreSQL: NewPostgresProvider(cfg),
			ProviderMongoDB:    NewMongoDBProvider(),
			ProviderMySQL:      NewMySQLProvider(cfg),
			ProviderSQLServer:  NewSQLServerProvider(cfg),
			ProviderOracle:     NewOracleProvider(),
		},
	}
}

func (f *ProviderFactory) Resolve(kind ProviderKind) (DBProvider, bool) {
	provider, ok := f.providers[kind]
	return provider, ok
}
