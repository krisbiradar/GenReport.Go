package db

import (
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoCache struct {
	mu      sync.RWMutex
	clients map[string]*mongo.Client
}

func NewMongoCache() *MongoCache {
	return &MongoCache{
		clients: make(map[string]*mongo.Client),
	}
}

func (c *MongoCache) GetOrCreate(key string, createFunc func() (*mongo.Client, error)) (*mongo.Client, error) {
	c.mu.RLock()
	if client, ok := c.clients[key]; ok {
		c.mu.RUnlock()
		return client, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.clients[key]; ok {
		return client, nil
	}

	client, err := createFunc()
	if err != nil {
		return nil, err
	}
	c.clients[key] = client
	return client, nil
}
