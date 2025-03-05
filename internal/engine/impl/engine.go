package impl

import (
	"context"

	"github.com/agnosticeng/agnostic-etl-engine/internal/engine"
	"github.com/agnosticeng/agnostic-etl-engine/internal/engine/impl/local"
	"github.com/agnosticeng/agnostic-etl-engine/internal/engine/impl/remote"
)

type EngineConfig struct {
	Local  *local.LocalEngineConfig
	Remote *remote.RemoteEngineConfig
}

func NewEngine(ctx context.Context, conf EngineConfig) (engine.Engine, error) {
	switch {
	case conf.Local != nil:
		return local.NewLocalEngine(ctx, *conf.Local)
	case conf.Remote != nil:
		return remote.NewRemoteEngine(ctx, *conf.Remote)
	default:
		return local.NewLocalEngine(ctx, local.LocalEngineConfig{})
	}
}
