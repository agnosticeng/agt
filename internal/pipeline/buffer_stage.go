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

type BufferStageConfig struct {
	Enter              *ch.QueryRef
	Leave              *ch.QueryRef
	Condition          *ch.QueryRef
	Queries            []ch.QueryRef
	MaxDuration        time.Duration
	ClickhouseSettings map[string]any
}

func (conf BufferStageConfig) WithDefaults() BufferStageConfig {
	return conf
}

func BufferStage(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan Vars,
	outchan chan<- Vars,
	conf BufferStageConfig,
) error {
	if len(conf.Queries) == 0 {
		return fmt.Errorf("loop must have at least 1 query")
	}

	if conf.MaxDuration == 0 && conf.Condition == nil {
		return fmt.Errorf("either MaxDuration or Condition must be set for the loop to finish")
	}

	var (
		logger           = slogctx.FromCtx(ctx)
		metricsScope     = tallyctx.FromContextOrNoop(ctx)
		procMetrics      = NewStageMetrics(metricsScope)
		enterMetrics     = conf.Enter.Metrics(metricsScope)
		leaveMetrics     = conf.Leave.Metrics(metricsScope)
		conditionMetrics = conf.Condition.Metrics(metricsScope)
		queriesMetrics   = lo.Map(conf.Queries, func(query ch.QueryRef, i int) *ch.QueryMetrics { return query.Metrics(metricsScope) })
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

		case vars, open := <-inchan:
			if !open {
				isInChanClosed = true
				break
			}

			if currentBatch == nil {
				currentBatch = newBatch(conf.MaxDuration)

				if conf.Enter != nil {
					if _, err := RunQuery(
						ctx,
						engine,
						tmpl,
						*conf.Enter,
						utils.MergeMaps(commonVars, vars),
						procMetrics,
						enterMetrics,
					); err != nil {
						return err
					}
				}
			}

			rows, err := RunQueries(
				ctx,
				engine,
				tmpl,
				conf.Queries,
				utils.MergeMaps(
					map[string]any{
						"LEFT":  currentBatch.getVars(),
						"RIGHT": vars,
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

			currentBatch.setVars(utils.LastElemOrDefault(rows, currentBatch.getVars()))

			if conf.Condition != nil {
				rows, err := RunQuery(
					ctx,
					engine,
					tmpl,
					*conf.Condition,
					utils.MergeMaps(currentBatch.getVars(), commonVars),
					procMetrics,
					conditionMetrics,
				)

				if err != nil {
					logger.Error(err.Error())
					return err
				}

				if len(rows) != 1 {
					return fmt.Errorf("condition query must return exactly 1 row: %d returned", len(rows))
				}

				v, found := rows[0]["value"].(*uint8)

				if !found || v == nil {
					return fmt.Errorf("condition query must return a single `value` column of type UInt8: returned %v", rows[0])
				}

				if *v > 0 {
					continue
				}
			}
		}

		if currentBatch != nil {
			if conf.Leave != nil {
				if _, err := RunQuery(
					ctx,
					engine,
					tmpl,
					*conf.Leave,
					utils.MergeMaps(currentBatch.getVars(), commonVars),
					procMetrics,
					leaveMetrics,
				); err != nil {
					return err
				}
			}

			select {
			case <-ctx.Done():
				return nil
			case outchan <- currentBatch.vars:
			}

			currentBatch = nil
		}

		if isInChanClosed {
			return nil
		}
	}
}

type batch struct {
	maxWaitTimer *time.Timer
	vars         Vars
}

func newBatch(maxWait time.Duration) *batch {
	var b batch

	if maxWait > 0 {
		b.maxWaitTimer = time.NewTimer(maxWait)
	}

	return &b
}

func (b *batch) getMaxWaitTimerChan() <-chan time.Time {
	if b == nil || b.maxWaitTimer == nil {
		return nil
	}

	return b.maxWaitTimer.C
}

func (b *batch) getVars() map[string]any {
	return b.vars
}

func (b *batch) setVars(vars map[string]any) {
	b.vars = vars
}
