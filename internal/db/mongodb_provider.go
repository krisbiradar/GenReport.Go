package db

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"genreport/internal/models"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDBProvider struct {
	cache *MongoCache
}

func NewMongoDBProvider() *MongoDBProvider {
	return &MongoDBProvider{
		cache: NewMongoCache(),
	}
}

func (p *MongoDBProvider) TestConnection(ctx context.Context, req models.TestConnectionRequest) error {
	connectionString := strings.TrimSpace(req.ConnectionString)
	if connectionString == "" {
		port := req.Port
		if port <= 0 {
			port = 27017
		}
		connectionString = buildMongoConnectionString(req.HostName, port, req.UserName, req.Password, req.DatabaseName)
	}

	client, err := p.cache.GetOrCreate(connectionString, func() (*mongo.Client, error) {
		clientOptions := options.Client().ApplyURI(connectionString)
		return mongo.Connect(ctx, clientOptions)
	})
	if err != nil {
		return err
	}

	return client.Ping(ctx, readpref.Primary())
}

func buildMongoConnectionString(host string, port int, user string, password string, database string) string {
	scheme := "mongodb"
	hostPort := host + ":" + strconv.Itoa(port)

	if strings.TrimSpace(user) != "" {
		userInfo := url.UserPassword(user, password).String()
		if strings.TrimSpace(database) != "" {
			return scheme + "://" + userInfo + "@" + hostPort + "/" + database
		}
		return scheme + "://" + userInfo + "@" + hostPort
	}

	if strings.TrimSpace(database) != "" {
		return scheme + "://" + hostPort + "/" + database
	}
	return scheme + "://" + hostPort
}
