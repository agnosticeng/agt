package pipeline

import (
	"context"
	"fmt"
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

type BatchProcessorConfig struct {
	Queries            []QueryFile
	SizeQuery          QueryFile
	MaxWait            time.Duration
	MaxSize            uint64
	ClickhouseSettings map[string]any
}

func (conf BatchProcessorConfig) WithDefaults() BatchProcessorConfig {
	return conf
}

func BatchProcessor(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf BatchProcessorConfig,
) error {
	if len(conf.Queries) == 0 {
		return fmt.Errorf("accumulate must have at least 1 query")
	}

	var (
		logger       = slogctx.FromCtx(ctx)
		stageMetrics = ch.NewStageMetrics(
			tallyctx.FromContextOrNoop(ctx),
			lo.Map(conf.Queries, func(q QueryFile, _ int) string {
				return q.Path
			}),
		)
		queriesMetrics = lo.Map(
			conf.Queries,
			func(query QueryFile, i int) *ch.QueryMetrics {
				return ch.NewQueryMetrics(tallyctx.FromContextOrNoop(ctx).Tagged(map[string]string{"query": query.Path}))
			},
		)
		sizeQueryMetrics = ch.NewQueryMetrics(
			tallyctx.FromContextOrNoop(ctx).Tagged(map[string]string{"query": conf.SizeQuery.Path}),
		)
	)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(ch.NormalizeSettings(conf.ClickhouseSettings)))
	}

	var (
		currentBatch      *Task
		currentBatchSize  uint64
		isInChanClosed    bool
		currentBatchTimer *time.Timer
	)

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-getTimerChan(currentBatchTimer):
			break

		case t, open := <-inchan:
			if !open {
				isInChanClosed = true
				break
			}

			if currentBatch == nil {
				currentBatch = &Task{}
				currentBatchSize = 0

				if conf.MaxWait > 0 {
					currentBatchTimer = time.NewTimer(conf.MaxWait)
				}
			}

			var vars = utils.MergeMaps(
				map[string]any{
					"LEFT":  currentBatch.Vars,
					"RIGHT": t.Vars,
				},
				commonVars,
			)

			for i, query := range conf.Queries {
				rows, err := ch.QueryTemplateWithMetricsAndLogger(
					ctx,
					engine,
					tmpl,
					query.Path,
					vars,
					stageMetrics,
					queriesMetrics[i],
					logger.With("query", query),
				)

				if err != nil && !query.IgnoreFailure {
					logger.Error(err.Error())
					return err
				}

				if i == (len(conf.Queries)-1) && len(rows) > 0 {
					currentBatch.Vars = rows[len(rows)-1]
				}
			}

			currentBatch.SequenceNumberStart = min(currentBatch.SequenceNumberStart, t.SequenceNumberStart)
			currentBatch.SequenceNumberEnd = max(currentBatch.SequenceNumberEnd, t.SequenceNumberEnd)

			if len(conf.SizeQuery.Path) == 0 {
				currentBatchSize++
			} else {
				rows, err := ch.QueryTemplateWithMetricsAndLogger(
					ctx,
					engine,
					tmpl,
					conf.SizeQuery.Path,
					utils.MergeMaps(currentBatch.Vars, commonVars),
					stageMetrics,
					sizeQueryMetrics,
					logger.With("query", conf.SizeQuery),
				)

				if err != nil {
					logger.Error(err.Error())
					return err
				}

				if len(rows) != 1 {
					return fmt.Errorf("size query must return exactly 1 row: %d returned", len(rows))
				}

				size, found := rows[0]["size"].(*uint64)

				if !found {
					return fmt.Errorf("size query must return a single `size` column of type UInt64: returned %v", rows[0])
				}

				currentBatchSize += *size
			}

			if currentBatchSize < conf.MaxSize {
				continue
			}
		}

		if currentBatch != nil {
			var t0 = time.Now()

			select {
			case <-ctx.Done():
				return nil
			case outchan <- currentBatch:
				stageMetrics.OutChanQueueTime.RecordDuration(time.Since(t0))
			}

			currentBatch = nil
			currentBatchSize = 0
			currentBatchTimer = nil
		}

		if isInChanClosed {
			return nil
		}
	}
}

func getTimerChan(t *time.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}

	return t.C
}
