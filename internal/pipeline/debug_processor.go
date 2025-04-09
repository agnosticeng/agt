package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	slogctx "github.com/veqryn/slog-context"
)

type DebugProcessorConfig struct {
	Pretty bool
}

func DebugProcessor(
	ctx context.Context,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf DebugProcessorConfig,
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

			select {
			case <-ctx.Done():
				return nil
			case outchan <- t:
				var js []byte

				if conf.Pretty {
					js, _ = json.MarshalIndent(t, "", "    ")
				} else {
					js, _ = json.Marshal(t)
				}

				fmt.Println(string(js))
			}
		}
	}
}
