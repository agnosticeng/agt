package pipeline

import (
	"context"
	"fmt"
	"text/template"

	"github.com/agnosticeng/agt/internal/engine"
	"github.com/uber-go/tally/v4"
)

type StageConfig struct {
	ChanSize int

	Execute *ExecuteStageConfig
	Debug   *DebugStageConfig
	Sleep   *SleepStageConfig
	Buffer  *BufferStageConfig
	Metrics *MetricsStageConfig
}

func (conf StageConfig) WithDefaults() StageConfig {
	if conf.ChanSize <= 0 {
		conf.ChanSize = 1
	}

	return conf
}

func Stage(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan Vars,
	outchan chan<- Vars,
	conf StageConfig,
) error {
	switch {
	case conf.Execute != nil:
		return ExecuteStage(ctx, engine, tmpl, commonVars, inchan, outchan, *conf.Execute)
	case conf.Debug != nil:
		return DebugStage(ctx, inchan, outchan, *conf.Debug)
	case conf.Sleep != nil:
		return SleepStage(ctx, inchan, outchan, *conf.Sleep)
	case conf.Buffer != nil:
		return BufferStage(ctx, engine, tmpl, commonVars, inchan, outchan, *conf.Buffer)
	case conf.Metrics != nil:
		return MetricsStage(ctx, engine, tmpl, commonVars, inchan, outchan, *conf.Metrics)
	default:
		return fmt.Errorf("unknwon processor type")
	}
}

type StageMetrics struct {
	Active tally.Gauge
}

func NewStageMetrics(scope tally.Scope) *StageMetrics {
	return &StageMetrics{
		Active: scope.Gauge("active"),
	}
}
