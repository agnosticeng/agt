package ch

import (
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/samber/lo"
)

type LogHandlerConfig struct {
	DiscardSources []string
}

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

func LogHandler(logger *slog.Logger, conf LogHandlerConfig) func(*clickhouse.Log) {
	return func(l *clickhouse.Log) {
		if lo.Contains(conf.DiscardSources, l.Source) {
			return
		}

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

func ProfileEventHandler(md *engine.QueryMetadata) func([]clickhouse.ProfileEvent) {
	return func(events []clickhouse.ProfileEvent) {
		for _, p := range events {
			if p.Name == "MemoryTrackerPeakUsage" {
				md.MemoryPeakUsage = max(md.MemoryPeakUsage, uint64(p.Value))
			}
		}
	}
}
