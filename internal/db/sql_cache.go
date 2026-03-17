package db

import (
	"database/sql"
	"sync"
)

type SQLCache struct {
	mu    sync.RWMutex
	conns map[string]*sql.DB
}

func NewSQLCache() *SQLCache {
	return &SQLCache{
		conns: make(map[string]*sql.DB),
	}
}

func (c *SQLCache) GetOrCreate(key string, createFunc func() (*sql.DB, error)) (*sql.DB, error) {
	c.mu.RLock()
	if dbConn, ok := c.conns[key]; ok {
		c.mu.RUnlock()
		return dbConn, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if dbConn, ok := c.conns[key]; ok {
		return dbConn, nil
	}

	dbConn, err := createFunc()
	if err != nil {
		return nil, err
	}
	c.conns[key] = dbConn
	return dbConn, nil
}
