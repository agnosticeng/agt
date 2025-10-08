package pipeline

import (
	"context"
	"time"

	"github.com/agnosticeng/concu/mapstream"
	slogctx "github.com/veqryn/slog-context"
)

type SleepStageConfig struct {
	Duration time.Duration
}

func SleepStage(
	ctx context.Context,
	inchan <-chan Vars,
	outchan chan<- Vars,
	conf SleepStageConfig,
) error {
	var logger = slogctx.FromCtx(ctx)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if conf.Duration == 0 {
		conf.Duration = time.Second
	}

	return mapstream.Mapper(
		ctx,
		inchan,
		outchan,
		func(ctx context.Context, vars Vars) (Vars, error) {
			select {
			case <-ctx.Done():
			case <-time.After(conf.Duration):
			}

			return vars, nil
		},
	)
}
