package pipeline

import (
	"context"
	"fmt"
	"text/template"
	"time"

	"github.com/agnosticeng/agt/internal/engine"
	"github.com/uber-go/tally/v4"
)

type ProcessorConfig struct {
	ChanSize int
	Workers  int

	Apply       *ApplyProcessorConfig
	Sequence    *SequenceProcessorConfig
	Debug       *DebugProcessorConfig
	Sleep       *SleepProcessorConfig
	Accumulate  *AccumulateProcessorConfig
	TaskMetrics *TaskMetricsProcessorConfig
}

func (conf ProcessorConfig) WithDefaults() ProcessorConfig {
	if conf.Workers <= 0 || conf.Sequence != nil || conf.Accumulate != nil {
		conf.Workers = 1
	}

	if conf.ChanSize <= 0 {
		conf.ChanSize = 1
	}

	return conf
}

func Processor(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf ProcessorConfig,
) error {
	switch {
	case conf.Apply != nil:
		return ApplyProcessor(ctx, engine, tmpl, commonVars, inchan, outchan, *conf.Apply)
	case conf.Sequence != nil:
		return SequenceProcessor(ctx, inchan, outchan, *conf.Sequence)
	case conf.Debug != nil:
		return DebugProcessor(ctx, inchan, outchan, *conf.Debug)
	case conf.Sleep != nil:
		return SleepProcessor(ctx, inchan, outchan, *conf.Sleep)
	case conf.Accumulate != nil:
		return AccumulateProcessor(ctx, engine, tmpl, commonVars, inchan, outchan, *conf.Accumulate)
	case conf.TaskMetrics != nil:
		return TaskMetricsProcessor(ctx, engine, tmpl, commonVars, inchan, outchan, *conf.TaskMetrics)
	default:
		return fmt.Errorf("unknwon processor type")
	}
}

type ProcessorMetrics struct {
	Active           tally.Gauge
	OutChanQueueTime tally.Histogram
}

func NewProcessorMetrics(scope tally.Scope) *ProcessorMetrics {
	return &ProcessorMetrics{
		Active: scope.Gauge("active"),
		OutChanQueueTime: scope.Histogram(
			"out_chan_queue_time",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*5, 2, 20),
		),
	}
}
