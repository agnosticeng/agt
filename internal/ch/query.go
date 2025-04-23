package ch

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/dustin/go-humanize"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
)

func IsDataQuery(query string) bool {
	query = strings.ToUpper(query)
	query = strings.TrimLeft(query, " ")

	switch {
	case strings.HasPrefix(query, "SELECT"):
		return true
	case strings.HasPrefix(query, "DESCRIBE"):
		return true
	default:
		return false
	}
}

func RowsToMaps(rows driver.Rows) ([]map[string]interface{}, error) {
	var (
		columnNames = rows.Columns()
		columnTypes = rows.ColumnTypes()
		res         []map[string]any
	)

	for rows.Next() {
		var (
			rowData = make([]any, len(columnTypes))
			item    = make(map[string]any)
		)

		for i := range columnTypes {
			rowData[i] = reflect.New(columnTypes[i].ScanType()).Interface()
		}

		if err := rows.Scan(rowData...); err != nil {
			return nil, err
		}

		for i, col := range rowData {
			item[columnNames[i]] = col
		}

		res = append(res, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func RunQuery(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	query QueryRef,
	vars map[string]any,
	procMetrics *ProcessorMetrics,
	queryMetrics *QueryMetrics,
) ([]map[string]any, error) {
	var (
		t0     = time.Now()
		logger = slogctx.FromCtx(ctx).With("query", query.Name)
	)

	q, err := utils.RenderTemplate(tmpl, query.Name, vars)

	if err != nil {
		return nil, fmt.Errorf("failed to render %s template: %w", query.Name, err)
	}

	if logger.Enabled(ctx, slog.Level(-10)) {
		logger.Log(ctx, -10, strings.ReplaceAll(q, "\n", " "), "template", q)
	}

	if procMetrics != nil {
		procMetrics.Active.Update(1)
		defer procMetrics.Active.Update(0)
	}

	res, md, err := engine.Query(ctx, q)

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
			return nil, fmt.Errorf("failed to execute query %s: %w", query.Name, err)
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

	return res, nil
}

func RunQueries(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	queries []QueryRef,
	vars map[string]any,
	procMetrics *ProcessorMetrics,
	queriesMetrics []*QueryMetrics,
) ([]map[string]any, error) {
	var resVars []map[string]any

	for i, query := range queries {
		var queryMetrics *QueryMetrics

		if len(queriesMetrics) > 0 {
			queryMetrics = queriesMetrics[i]
		}

		rows, err := RunQuery(
			ctx,
			engine,
			tmpl,
			query,
			vars,
			procMetrics,
			queryMetrics,
		)

		if err != nil {
			return nil, err
		}

		if len(rows) > 0 {
			resVars = rows
		}
	}

	return resVars, nil
}
