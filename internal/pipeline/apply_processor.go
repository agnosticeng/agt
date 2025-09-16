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

type ApplyProcessorConfig struct {
	Queries            []ch.QueryRef
	ClickhouseSettings map[string]any
}

func (conf ApplyProcessorConfig) WithDefaults() ApplyProcessorConfig {
	return conf
}

func ApplyProcessor(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf ApplyProcessorConfig,
) error {
	var (
		logger         = slogctx.FromCtx(ctx)
		metricsScope   = tallyctx.FromContextOrNoop(ctx)
		procMetrics    = NewProcessorMetrics(metricsScope)
		queriesMetrics = lo.Map(conf.Queries, func(query ch.QueryRef, i int) *ch.QueryMetrics { return query.Metrics(metricsScope) })
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

			rows, err := RunQueries(
				ctx,
				engine,
				tmpl,
				conf.Queries,
				utils.MergeMaps(
					map[string]any{"TASK_ID": t.Id()},
					t.Vars,
					commonVars,
				),
				procMetrics,
				queriesMetrics,
			)

			if err != nil {
				return err
			}

			t.Vars = utils.LastElemOrDefault(rows, t.Vars)
			var t0 = time.Now()

			select {
			case <-ctx.Done():
				return nil
			case outchan <- t:
				procMetrics.OutChanQueueTime.RecordDuration(time.Since(t0))
			}
		}
	}
}
