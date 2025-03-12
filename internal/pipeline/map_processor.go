package pipeline

import (
	"context"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/agnosticeng/tallyctx"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
)

type MapProcessorConfig struct {
	Queries            []QueryFile
	ClickhouseSettings map[string]any
}

func (conf MapProcessorConfig) WithDefaults() MapProcessorConfig {
	return conf
}

func MapProcessor(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf MapProcessorConfig,
) error {
	var (
		logger       = slogctx.FromCtx(ctx)
		stageMetrics = ch.NewStageMetrics(
			tallyctx.FromContextOrNoop(ctx),
			lo.Map(conf.Queries, func(f QueryFile, _ int) string {
				return f.Path
			}),
		)
		queriesMetrics = lo.Map(
			conf.Queries,
			func(query QueryFile, i int) *ch.QueryMetrics {
				return ch.NewQueryMetrics(tallyctx.FromContextOrNoop(ctx).Tagged(map[string]string{"query": query.Path}))
			},
		)
	)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(ch.NormalizeSettings(conf.ClickhouseSettings)))
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case t, open := <-inchan:
			if !open {
				return nil
			}

			for i, query := range conf.Queries {
				rows, err := ch.QueryTemplateWithMetricsAndLogger(
					ctx,
					engine,
					tmpl,
					query.Path,
					utils.MergeMaps(t.Vars, commonVars),
					stageMetrics,
					queriesMetrics[i],
					logger.With("query", query),
				)

				if err != nil && !query.IgnoreFailure {
					logger.Error(err.Error())
					return err
				}

				if i == (len(conf.Queries)-1) && len(rows) > 0 {
					t.Vars = rows[len(rows)-1]
				}
			}

			var t0 = time.Now()

			select {
			case <-ctx.Done():
				return nil
			case outchan <- t:
				stageMetrics.OutChanQueueTime.RecordDuration(time.Since(t0))
			}
		}
	}
}
