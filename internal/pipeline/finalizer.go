package pipeline

import (
	"context"

	slogctx "github.com/veqryn/slog-context"
)

type FinalizerConfig struct{}

func Finalizer(
	ctx context.Context,
	inchan <-chan *Task,
	conf FinalizerConfig,
) error {
	var logger = slogctx.FromCtx(ctx)

	logger.Debug("started")
	defer logger.Debug("stopped")

	for {
		select {
		case <-ctx.Done():
			return nil
		case t, open := <-inchan:
			if !open {
				return nil
			}

			logger.Info("task finalized", taskToKeyValues(t)...)
		}
	}
}
