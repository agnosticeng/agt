package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/agnosticeng/concu/mapstream"
	slogctx "github.com/veqryn/slog-context"
)

type DebugStageConfig struct {
	Id     string
	Pretty bool
}

func DebugStage(
	ctx context.Context,
	inchan <-chan Vars,
	outchan chan<- Vars,
	conf DebugStageConfig,
) error {
	var logger = slogctx.FromCtx(ctx)

	logger.Debug("started")
	defer logger.Debug("stopped")

	return mapstream.Mapper(
		ctx,
		inchan,
		outchan,
		func(ctx context.Context, vars Vars) (Vars, error) {
			var js []byte

			if conf.Pretty {
				js, _ = json.MarshalIndent(vars, "", "    ")
			} else {
				js, _ = json.Marshal(vars)
			}

			if len(conf.Id) > 0 {
				fmt.Println(conf.Id, string(js))
			} else {
				fmt.Println(string(js))
			}

			return vars, nil
		},
	)
}
