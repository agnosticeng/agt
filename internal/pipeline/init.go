package pipeline

import (
	"context"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	slogctx "github.com/veqryn/slog-context"
)

type InitConfig struct {
	Queries            []ch.QueryRef
	ClickhouseSettings map[string]any
}

func (conf InitConfig) WithDefaults() InitConfig {
	return conf
}

func Init(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	vars map[string]interface{},
	conf InitConfig,
) (map[string]any, error) {
	var logger = slogctx.FromCtx(ctx)

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(ch.NormalizeSettings(conf.ClickhouseSettings)))
	}

	rows, err := ch.RunQueries(ctx, engine, tmpl, conf.Queries, vars, nil, nil)

	if err != nil {
		return nil, err
	}

	logger.Info("init vars", varsToKeyValues(utils.LastElemOrZero(rows))...)
	return utils.LastElemOrZero(rows), nil
}
