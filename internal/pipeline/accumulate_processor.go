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

type AccumulateProcessorConfig struct {
	Setup              *ch.QueryRef
	Queries            []ch.QueryRef
	Size               *ch.QueryRef
	Teardown           *ch.QueryRef
	MaxWait            time.Duration
	MaxSize            uint64
	ClickhouseSettings map[string]any
}

func (conf AccumulateProcessorConfig) WithDefaults() AccumulateProcessorConfig {
	return conf
}

func AccumulateProcessor(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf AccumulateProcessorConfig,
) error {
	if len(conf.Queries) == 0 {
		return fmt.Errorf("accumulate must have at least 1 query")
	}

	var (
		logger          = slogctx.FromCtx(ctx)
		metricsScope    = tallyctx.FromContextOrNoop(ctx)
		procMetrics     = ch.NewProcessorMetrics(metricsScope)
		queriesMetrics  = lo.Map(conf.Queries, func(query ch.QueryRef, i int) *ch.QueryMetrics { return query.Metrics(metricsScope) })
		setupMetrics    = conf.Setup.Metrics(metricsScope)
		sizeMetrics     = conf.Size.Metrics(metricsScope)
		teardownMetrics = conf.Teardown.Metrics(metricsScope)
	)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(ch.NormalizeSettings(conf.ClickhouseSettings)))
	}

	var (
		currentBatch   *batch
		isInChanClosed bool
	)

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-currentBatch.getMaxWaitTimerChan():
			break

		case t, open := <-inchan:
			if !open {
				isInChanClosed = true
				break
			}

			if currentBatch == nil {
				currentBatch = newBatch(conf.MaxSize, conf.MaxWait)

				if conf.Setup != nil {
					if _, err := ch.RunQuery(
						ctx,
						engine,
						tmpl,
						*conf.Setup,
						utils.MergeMaps(commonVars, t.Vars),
						procMetrics,
						setupMetrics,
					); err != nil {
						return err
					}
				}
			}

			rows, err := ch.RunQueries(
				ctx,
				engine,
				tmpl,
				conf.Queries,
				utils.MergeMaps(
					map[string]any{
						"LEFT":  currentBatch.getVars(),
						"RIGHT": t.Vars,
					},
					commonVars,
				),
				procMetrics,
				queriesMetrics,
			)

			if err != nil {
				logger.Error(err.Error())
				return err
			}

			currentBatch.addTask(t)
			currentBatch.setVars(utils.LastElemOrDefault(rows, currentBatch.getVars()))

			if conf.Size == nil {
				currentBatch.incrementSize(1)
			} else {
				rows, err := ch.RunQuery(
					ctx,
					engine,
					tmpl,
					*conf.Size,
					utils.MergeMaps(currentBatch.getVars(), commonVars),
					procMetrics,
					sizeMetrics,
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

				currentBatch.setSize(*size)
				logger.Debug("size query run", "size", *size)
			}

			if !currentBatch.hasReachedMaxSize() {
				continue
			}
		}

		if currentBatch != nil {
			var t0 = time.Now()

			if conf.Teardown != nil {
				if _, err := ch.RunQuery(
					ctx,
					engine,
					tmpl,
					*conf.Teardown,
					utils.MergeMaps(currentBatch.getVars(), commonVars),
					procMetrics,
					teardownMetrics,
				); err != nil {
					return err
				}
			}

			select {
			case <-ctx.Done():
				return nil
			case outchan <- currentBatch.toTask():
				logger.Info("yield batch", "size", currentBatch.getSize())
				procMetrics.OutChanQueueTime.RecordDuration(time.Since(t0))
			}

			currentBatch = nil
		}

		if isInChanClosed {
			return nil
		}
	}
}

type batch struct {
	maxSize             uint64
	maxWaitTimer        *time.Timer
	size                uint64
	sequenceNumberStart *int64
	sequenceNumberEnd   *int64
	vars                map[string]any
}

func newBatch(maxSize uint64, maxWait time.Duration) *batch {
	var b batch
	b.maxSize = maxSize

	if maxWait > 0 {
		b.maxWaitTimer = time.NewTimer(maxWait)
	}

	return &b
}

func (b *batch) hasReachedMaxSize() bool {
	return b.size >= b.maxSize
}

func (b *batch) getMaxWaitTimerChan() <-chan time.Time {
	if b == nil || b.maxWaitTimer == nil {
		return nil
	}

	return b.maxWaitTimer.C
}

func (b *batch) addTask(t *Task) {
	if b.sequenceNumberStart == nil {
		b.sequenceNumberStart = utils.Ptr(t.SequenceNumberStart)
	} else {
		b.sequenceNumberStart = utils.Ptr(min(*b.sequenceNumberStart, t.SequenceNumberStart))
	}

	if b.sequenceNumberEnd == nil {
		b.sequenceNumberEnd = &t.SequenceNumberEnd
	} else {
		b.sequenceNumberEnd = utils.Ptr(max(*b.sequenceNumberEnd, t.SequenceNumberEnd))
	}
}

func (b *batch) getSize() uint64 {
	return b.size
}

func (b *batch) incrementSize(incr uint64) {
	b.size = b.size + incr
}

func (b *batch) setSize(size uint64) {
	b.size = size
}

func (b *batch) getVars() map[string]any {
	return b.vars
}

func (b *batch) setVars(vars map[string]any) {
	b.vars = vars
}

func (b *batch) toTask() *Task {
	return &Task{
		SequenceNumberStart: *b.sequenceNumberStart,
		SequenceNumberEnd:   *b.sequenceNumberEnd,
		Vars:                b.vars,
	}
}
