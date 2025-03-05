package pipeline

import (
	"context"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agnostic-etl-engine/internal/ch"
	"github.com/agnosticeng/agnostic-etl-engine/internal/engine"
	"github.com/agnosticeng/agnostic-etl-engine/internal/utils"
	slogctx "github.com/veqryn/slog-context"
)

type SourceConfig struct {
	Query              string
	PollInterval       time.Duration
	StopAfter          int
	StopOnEmpty        bool
	ClickhouseSettings map[string]any
}

func Source(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	vars map[string]interface{},
	outchan chan<- *Task,
	conf SourceConfig,
) error {
	var (
		logger             = slogctx.FromCtx(ctx)
		nextWaitDuration   time.Duration
		nextSequenceNumber int64
		lastRow            map[string]any
		iterations         int
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
		case <-time.After(nextWaitDuration):
			rows, _, err := ch.QueryFromTemplate(
				ctx,
				engine,
				tmpl,
				conf.Query,
				utils.MergeMaps(vars, lastRow),
			)

			if err != nil {
				return err
			}

			if len(rows) == 0 {
				if conf.StopOnEmpty {
					return nil
				}

				nextWaitDuration = conf.PollInterval
				continue
			}

			for _, row := range rows {
				var t = Task{
					SequenceNumberStart: nextSequenceNumber,
					SequenceNumberEnd:   nextSequenceNumber,
					Vars:                utils.MergeMaps(vars, row),
				}

				select {
				case <-ctx.Done():
					return nil
				case outchan <- &t:
				}

				nextSequenceNumber++
			}
			iterations++
			nextWaitDuration = conf.PollInterval
			lastRow = rows[len(rows)-1]

			if conf.StopAfter > 0 && iterations == conf.StopAfter {
				return nil
			}
		}
	}
}
