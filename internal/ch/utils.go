package ch

import (
	"context"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/iancoleman/strcase"
)

func NormalizeSettings(settings clickhouse.Settings) clickhouse.Settings {
	var m = make(clickhouse.Settings)

	for k, v := range settings {
		m[strcase.ToSnake(k)] = v
	}

	return m
}

func LogQueryMetadata(ctx context.Context, logger *slog.Logger, level slog.Level, msg string, md *engine.QueryMetadata) {
	if logger.Enabled(ctx, level) {
		logger.Log(
			ctx,
			level,
			msg,
			"rows", md.Rows,
			"bytes", md.Bytes,
			"total_rows", md.TotalRows,
			"wrote_rows", md.WroteRows,
			"wrote_bytes", md.WroteBytes,
			"elapsed", md.Elapsed,
		)
	}
}
