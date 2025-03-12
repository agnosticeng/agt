package pipeline

import (
	"context"
	"fmt"
	"text/template"

	"github.com/agnosticeng/agt/internal/engine"
)

type ProcessorConfig struct {
	ChanSize int
	Workers  int

	Map   *MapProcessorConfig
	Seq   *SeqProcessorConfig
	Debug *DebugProcessorConfig
	Sleep *SleepProcessorConfig
	Batch *BatchProcessorConfig
}

func (conf ProcessorConfig) WithDefaults() ProcessorConfig {
	if conf.Workers <= 0 {
		conf.Workers = 1
	}

	if conf.Seq != nil {
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
	case conf.Map != nil:
		return MapProcessor(
			ctx,
			engine,
			tmpl,
			commonVars,
			inchan,
			outchan,
			*conf.Map,
		)

	case conf.Seq != nil:
		return SeqProcessor(
			ctx,
			inchan,
			outchan,
			*conf.Seq,
		)

	case conf.Debug != nil:
		return DebugProcessor(
			ctx,
			inchan,
			outchan,
			*conf.Debug,
		)

	case conf.Sleep != nil:
		return SleepProcessor(
			ctx,
			inchan,
			outchan,
			*conf.Sleep,
		)

	case conf.Batch != nil:
		return BatchProcessor(
			ctx,
			engine,
			tmpl,
			commonVars,
			inchan,
			outchan,
			*conf.Batch,
		)

	default:
		return fmt.Errorf("unknwon processor type")
	}
}
