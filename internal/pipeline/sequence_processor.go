package pipeline

import (
	"context"

	slogctx "github.com/veqryn/slog-context"
)

type SequenceProcessorConfig struct{}

func SequenceProcessor(
	ctx context.Context,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf SequenceProcessorConfig,
) error {
	var (
		buf                Tasks
		nextSequenceNumber int64
		logger             = slogctx.FromCtx(ctx)
	)

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

			buf.Insert(t)

			for {
				if len(buf) == 0 {
					break
				}

				t = buf[0]

				if t.SequenceNumberStart != nextSequenceNumber {
					break
				}

				select {
				case <-ctx.Done():
					return nil
				case outchan <- t:
					logger.Debug("task sequenced", taskToKeyValues(t)...)
				}

				nextSequenceNumber = t.SequenceNumberEnd + 1
				buf = buf[1:]
			}

		}
	}
}
