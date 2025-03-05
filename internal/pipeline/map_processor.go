package pipeline

import (
	"context"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agnostic-etl-engine/internal/ch"
	"github.com/agnosticeng/agnostic-etl-engine/internal/engine"
	"github.com/agnosticeng/tallyctx"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
)

type MapProcessorConfig struct {
	Queries            []string
	ClickhouseSettings map[string]any
}

func (conf MapProcessorConfig) WithDefaults() MapProcessorConfig {
	return conf
}

func MapProcessor(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf MapProcessorConfig,
) error {
	var (
		logger         = slogctx.FromCtx(ctx)
		stageMetrics   = ch.NewStageMetrics(tallyctx.FromContextOrNoop(ctx), conf.Queries)
		queriesMetrics = lo.Map(conf.Queries, func(query string, i int) *ch.QueryMetrics {
			return ch.NewQueryMetrics(tallyctx.FromContextOrNoop(ctx).Tagged(map[string]string{"query": query}))
		})
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
					query,
					t.Vars,
					stageMetrics,
					queriesMetrics[i],
					logger.With("query", query),
				)

				if err != nil {
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
