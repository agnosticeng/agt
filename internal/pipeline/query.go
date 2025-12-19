package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/dustin/go-humanize"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
)

func RunQuery(
	ctx context.Context,
	eng engine.Engine,
	tmpl *template.Template,
	query ch.QueryRef,
	vars map[string]any,
	procMetrics *StageMetrics,
	queryMetrics *ch.QueryMetrics,
) ([]map[string]any, *engine.QueryMetadata, error) {
	var (
		t0     = time.Now()
		logger = slogctx.FromCtx(ctx).With("query", query.Name)
	)

	q, err := utils.RenderTemplate(tmpl, query.Name, vars)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to render %s template: %w", query.Name, err)
	}

	if logger.Enabled(ctx, slog.Level(-10)) {
		logger.Log(ctx, -10, strings.ReplaceAll(q, "\n", " "), "template", q)
	}

	if procMetrics != nil {
		procMetrics.Active.Update(1)
		defer procMetrics.Active.Update(0)
	}

	res, md, err := eng.Query(ctx, q)

	logger.Debug(
		"summary",
		"rows", md.Rows,
		"bytes", md.Bytes,
		"total_rows", md.TotalRows,
		"wrote_rows", md.WroteRows,
		"wrote_bytes", md.WroteBytes,
		"elapsed", md.Elapsed,
		"memory_peak_usage", humanize.Bytes(md.MemoryPeakUsage),
	)

	if err != nil && !query.IgnoreFailure {
		if ex, ok := lo.ErrorsAs[*proto.Exception](err); !ok || !lo.Contains(query.IgnoreErrorCodes, int(ex.Code)) {
			js, _ := json.Marshal(redactSensitiveVars(vars))
			return nil, nil, fmt.Errorf("failed to execute query %s(vars=%v): %w", query.Name, string(js), err)
		}
	}

	if queryMetrics != nil {
		queryMetrics.ExecutionTime.RecordDuration(time.Since(t0))
		queryMetrics.Elapsed.RecordDuration(md.Elapsed)
		queryMetrics.Rows.Inc(int64(md.Rows))
		queryMetrics.Bytes.Inc(int64(md.Bytes))
		queryMetrics.TotalRows.Inc(int64(md.TotalRows))
		queryMetrics.WroteRows.Inc(int64(md.WroteRows))
		queryMetrics.WroteBytes.Inc(int64(md.WroteBytes))
		queryMetrics.MemoryPeakUsage.Update(float64(md.MemoryPeakUsage))
	}

	return res, md, nil
}

func RunQueries(
	ctx context.Context,
	eng engine.Engine,
	tmpl *template.Template,
	queries []ch.QueryRef,
	vars map[string]any,
	procMetrics *StageMetrics,
	queriesMetrics []*ch.QueryMetrics,
) ([]map[string]any, *engine.QueryMetadata, error) {
	var (
		resVars []map[string]any
		resMd   engine.QueryMetadata
	)

	for i, query := range queries {
		var queryMetrics *ch.QueryMetrics

		if len(queriesMetrics) > 0 {
			queryMetrics = queriesMetrics[i]
		}

		rows, md, err := RunQuery(
			ctx,
			eng,
			tmpl,
			query,
			vars,
			procMetrics,
			queryMetrics,
		)

		if err != nil {
			return nil, nil, err
		}

		if len(rows) > 0 && !query.IgnoreOutput {
			resVars = rows
		}

		resMd.Merge(md)
	}

	return resVars, &resMd, nil
}
