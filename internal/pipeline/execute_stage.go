package pipeline

import (
	"context"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/agnosticeng/concu/mapstream"
	"github.com/agnosticeng/tallyctx"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
)

type ExecuteStageConfig struct {
	mapstream.MapStreamConfig
	Queries            []ch.QueryRef
	ClickhouseSettings map[string]any
}

func (conf ExecuteStageConfig) WithDefaults() ExecuteStageConfig {
	return conf
}

func ExecuteStage(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan Vars,
	outchan chan<- Vars,
	conf ExecuteStageConfig,
) error {
	var (
		logger         = slogctx.FromCtx(ctx)
		metricsScope   = tallyctx.FromContextOrNoop(ctx)
		procMetrics    = NewStageMetrics(metricsScope)
		queriesMetrics = lo.Map(conf.Queries, func(query ch.QueryRef, i int) *ch.QueryMetrics { return query.Metrics(metricsScope) })
	)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(ch.NormalizeSettings(conf.ClickhouseSettings)))
	}

	return mapstream.MapStream(
		ctx,
		inchan,
		outchan,
		func(ctx context.Context, vars Vars) (Vars, error) {
			rows, err := RunQueries(
				ctx,
				engine,
				tmpl,
				conf.Queries,
				utils.MergeMaps(vars, commonVars),
				procMetrics,
				queriesMetrics,
			)

			if err != nil {
				return nil, err
			}

			return utils.LastElemOrDefault(rows, vars), nil
		},
		conf.MapStreamConfig,
	)
}
