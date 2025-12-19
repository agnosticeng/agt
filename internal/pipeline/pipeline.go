package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"text/template"

	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/agnosticeng/tallyctx"
	"github.com/google/uuid"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
	"golang.org/x/sync/errgroup"
)

type PipelineConfig struct {
	Init      InitConfig
	Source    SourceConfig
	Stages    []StageConfig
	Finalizer FinalizerConfig
}

func (conf PipelineConfig) WithDefaults() PipelineConfig {
	conf.Init = conf.Init.WithDefaults()
	conf.Stages = lo.Map(conf.Stages, func(conf StageConfig, _ int) StageConfig { return conf.WithDefaults() })
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

	if len(conf.Stages) == 0 {
		return fmt.Errorf("pipeline must have at leats 1 stage")
	}

	runUUID, err := uuid.NewV7()

	if err != nil {
		return err
	}

	vars["UUID"] = runUUID.String()

	initVars, err := Init(ctx, engine, tmpl, vars, conf.Init)

	if err != nil {
		return err
	}

	vars = utils.MergeMaps(vars, initVars)

	logger.Info("pipeline initialized")

	var (
		group, groupctx = errgroup.WithContext(ctx)
		sourceOutChan   = make(chan Vars, 3)
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

	for i, procConfig := range conf.Stages {
		var (
			inchan  = lastOutChan
			outchan = make(chan Vars, procConfig.ChanSize)
		)

		group.Go(func() error {
			defer close(outchan)
			var procCtx = slogctx.With(groupctx, "stage", i)
			procCtx = tallyctx.NewContext(
				procCtx, tallyctx.FromContextOrNoop(procCtx).
					SubScope("stage").
					Tagged(map[string]string{
						"stage": strconv.FormatInt(int64(i), 10),
					}),
			)

			return Stage(procCtx, engine, tmpl, vars, inchan, outchan, procConfig)
		})

		lastOutChan = outchan
	}

	group.Go(func() error {
		var finalizerCtx = tallyctx.NewContext(groupctx, tallyctx.FromContextOrNoop(groupctx).SubScope("finalizer"))
		return Finalizer(finalizerCtx, lastOutChan, conf.Finalizer)
	})

	return group.Wait()
}
