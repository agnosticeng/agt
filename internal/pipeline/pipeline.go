package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"text/template"

	"github.com/agnosticeng/agnostic-etl-engine/internal/engine"
	"github.com/agnosticeng/concu/worker"
	"github.com/agnosticeng/tallyctx"
	"github.com/google/uuid"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
	"golang.org/x/sync/errgroup"
)

type PipelineConfig struct {
	Init       InitConfig
	Source     SourceConfig
	Processors []ProcessorConfig
	Finalizer  FinalizerConfig
}

func (conf PipelineConfig) WithDefaults() PipelineConfig {
	conf.Init = conf.Init.WithDefaults()
	conf.Processors = lo.Map(conf.Processors, func(conf ProcessorConfig, _ int) ProcessorConfig { return conf.WithDefaults() })
	return conf
}

func Run(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	vars map[string]interface{},
	conf PipelineConfig,
) error {
	var logger = slogctx.FromCtx(ctx)
	defer logger.Info("pipeline finished running")

	if len(conf.Processors) == 0 {
		return fmt.Errorf("pipeline must have at leats 1 processor")
	}

	runUUID, err := uuid.NewV7()

	if err != nil {
		return err
	}

	vars["UUID"] = runUUID.String()

	if err := Init(ctx, engine, tmpl, vars, conf.Init); err != nil {
		return err
	}

	logger.Info("pipeline initialized")

	var (
		group, groupctx = errgroup.WithContext(ctx)
		sourceOutChan   = make(chan *Task, 3)
		lastOutChan     = sourceOutChan
	)

	group.Go(func() error {
		defer close(sourceOutChan)

		var sourceCtx = tallyctx.NewContext(groupctx, tallyctx.FromContextOrNoop(groupctx).SubScope("source"))

		return Source(
			sourceCtx,
			engine,
			tmpl,
			vars,
			sourceOutChan,
			conf.Source,
		)
	})

	for i, procConfig := range conf.Processors {
		var (
			inchan  = lastOutChan
			outchan = make(chan *Task, procConfig.ChanSize)
		)

		group.Go(func() error {
			defer close(outchan)

			return worker.RunN(
				groupctx,
				procConfig.Workers,
				func(ctx context.Context, j int) func() error {
					return func() error {
						var stepCtx = slogctx.With(
							ctx,
							"processor", i,
							"worker", j,
						)

						stepCtx = tallyctx.NewContext(
							stepCtx,
							tallyctx.FromContextOrNoop(stepCtx).
								SubScope("processor").
								Tagged(map[string]string{
									"processor": strconv.FormatInt(int64(i), 10),
									"worker":    strconv.FormatInt(int64(j), 10),
								}),
						)

						return Processor(
							stepCtx,
							engine,
							tmpl,
							inchan,
							outchan,
							procConfig,
						)
					}
				},
			)
		})

		lastOutChan = outchan
	}

	group.Go(func() error {
		var finalizerCtx = tallyctx.NewContext(groupctx, tallyctx.FromContextOrNoop(groupctx).SubScope("finalizer"))
		return Finalizer(finalizerCtx, lastOutChan, conf.Finalizer)
	})

	return group.Wait()
}
