package engine

import (
	"context"
	"time"
)

type Engine interface {
	Start() error
	Stop()
	Wait() error
	Ping(ctx context.Context) error
	Query(ctx context.Context, query string, args ...any) ([]map[string]any, *QueryMetadata, error)
}

type QueryMetadata struct {
	Rows       uint64
	Bytes      uint64
	TotalRows  uint64
	WroteRows  uint64
	WroteBytes uint64
	Elapsed    time.Duration
}

type Log struct {
	Time     time.Time
	Hostname string
	QueryID  string
	ThreadID uint64
	Priority int8
	Source   string
	Text     string
}
