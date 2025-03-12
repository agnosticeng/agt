package ch

import (
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/agnosticeng/agt/internal/engine"
)

func ProgressHandler(md *engine.QueryMetadata) func(*proto.Progress) {
	return func(p *proto.Progress) {
		if p == nil {
			return
		}

		md.Rows += p.Rows
		md.Bytes += p.Bytes
		md.TotalRows += p.TotalRows
		md.WroteRows += p.WroteRows
		md.WroteBytes += p.WroteBytes
		md.Elapsed += p.Elapsed
	}
}

func LogHandler(logger *slog.Logger) func(*clickhouse.Log) {
	return func(l *clickhouse.Log) {
		logger.Info(
			l.Text,
			"hostname", l.Hostname,
			"query_id", l.QueryID,
			"thread_id", l.ThreadID,
			"priority", l.Priority,
			"source", l.Source,
		)
	}
}
