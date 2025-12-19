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
	Rows            uint64
	Bytes           uint64
	TotalRows       uint64
	WroteRows       uint64
	WroteBytes      uint64
	Elapsed         time.Duration
	MemoryPeakUsage uint64
}

func (md *QueryMetadata) Merge(other *QueryMetadata) {
	md.Rows += other.Rows
	md.Bytes += other.Bytes
	md.TotalRows += other.TotalRows
	md.WroteRows += other.WroteRows
	md.WroteBytes += other.WroteBytes
	md.Elapsed += other.Elapsed
	md.MemoryPeakUsage = max(md.MemoryPeakUsage, other.MemoryPeakUsage)
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
