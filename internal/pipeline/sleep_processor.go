package pipeline

import (
	"context"
	"time"

	slogctx "github.com/veqryn/slog-context"
)

type SleepProcessorConfig struct {
	Duration time.Duration
}

func SleepProcessor(
	ctx context.Context,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf SleepProcessorConfig,
) error {
	var logger = slogctx.FromCtx(ctx)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if conf.Duration == 0 {
		conf.Duration = time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case t, open := <-inchan:
			if !open {
				return nil
			}

			select {
			case <-ctx.Done():
			case <-time.After(conf.Duration):
			}

			select {
			case <-ctx.Done():
				return nil
			case outchan <- t:
			}
		}
	}
}
