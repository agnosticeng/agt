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
	"github.com/agnosticeng/agnostic-etl-engine/internal/engine"
	"github.com/agnosticeng/agnostic-etl-engine/internal/utils"
	slogctx "github.com/veqryn/slog-context"
)

func IsSelectQuery(query string) bool {
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

func QueryFromTemplate(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	name string,
	vars map[string]any,
) ([]map[string]any, *engine.QueryMetadata, error) {
	var (
		logger = slogctx.FromCtx(ctx)
		res    []map[string]any
	)

	q, err := utils.RenderTemplate(tmpl, name, vars)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	if logger.Enabled(ctx, slog.Level(-10)) {
		logger.Log(ctx, -10, q, "template", name)
	}

	res, md, err := engine.Query(ctx, q)

	LogQueryMetadata(ctx, logger, slog.LevelDebug, name, md)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute template %s: %w", name, err)
	}

	return res, md, nil
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

func QueryTemplateWithMetricsAndLogger(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	name string,
	vars map[string]any,
	stageMetrics *StageMetrics,
	queryMetrics *QueryMetrics,
	logger *slog.Logger,
) ([]map[string]any, error) {
	var t0 = time.Now()
	stageMetrics.Active.Update(1)
	defer stageMetrics.Active.Update(0)

	rows, md, err := QueryFromTemplate(
		ctx,
		engine,
		tmpl,
		name,
		vars,
	)

	if err != nil {
		return nil, err
	}

	queryMetrics.ExecutionTime.RecordDuration(time.Since(t0))
	queryMetrics.Elapsed.RecordDuration(md.Elapsed)
	queryMetrics.Rows.Inc(int64(md.Rows))
	queryMetrics.Bytes.Inc(int64(md.Bytes))
	queryMetrics.TotalRows.Inc(int64(md.TotalRows))
	queryMetrics.WroteRows.Inc(int64(md.WroteRows))
	queryMetrics.WroteBytes.Inc(int64(md.WroteBytes))

	logger.Debug(name, "duration", time.Since(t0))
	return rows, nil
}
