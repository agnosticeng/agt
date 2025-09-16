package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/agnosticeng/tallyctx"
	"github.com/uber-go/tally/v4"
)

type MetricType string

var (
	MetricCounter MetricType = "COUNTER"
	MetricGauge   MetricType = "GAUGE"
)

type TaskMetricConfig struct {
	Name string
	Type MetricType
}

type TaskMetricsProcessorConfig struct {
	Metrics            []TaskMetricConfig
	Query              ch.QueryRef
	ClickhouseSettings map[string]any
}

func (conf TaskMetricsProcessorConfig) WithDefaults() TaskMetricsProcessorConfig {
	return conf
}

func TaskMetricsProcessor(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	commonVars map[string]any,
	inchan <-chan *Task,
	outchan chan<- *Task,
	conf TaskMetricsProcessorConfig,
) error {
	var (
		metricsScope = tallyctx.FromContextOrNoop(ctx)
		procMetrics  = NewProcessorMetrics(metricsScope)
		queryMetrics = conf.Query.Metrics(metricsScope)
		taskMetrics  = make(map[string]any)
	)

	for _, conf := range conf.Metrics {
		switch conf.Type {
		case MetricCounter:
			taskMetrics[conf.Name] = metricsScope.Counter(conf.Name)
		case MetricGauge:
			taskMetrics[conf.Name] = metricsScope.Gauge(conf.Name)
		default:
			return fmt.Errorf("unknown metric type: %v", conf.Type)
		}
	}

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(ch.NormalizeSettings(conf.ClickhouseSettings)))
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case t, open := <-inchan:
			if !open {
				return nil
			}

			rows, err := RunQuery(
				ctx,
				engine,
				tmpl,
				conf.Query,
				utils.MergeMaps(
					map[string]any{"TASK_ID": t.Id()},
					t.Vars,
					commonVars,
				),
				procMetrics,
				queryMetrics,
			)

			if err != nil {
				return err
			}

			if len(rows) != 1 {
				return fmt.Errorf("task metric query must return exactly 1 row: %d returned", len(rows))
			}

			for k, v := range taskMetrics {
				switch v := v.(type) {
				case tally.Counter:
					if i, err := tryGetFromRow[int64](rows[0], k); err != nil {
						return err
					} else {
						v.Inc(i)
					}
				case tally.Gauge:
					if f, err := tryGetFromRow[float64](rows[0], k); err != nil {
						return err
					} else {
						v.Update(f)
					}
				default:
					return fmt.Errorf("unknown metric type: %v", reflect.TypeOf(v))
				}
			}

			select {
			case <-ctx.Done():
				return nil
			case outchan <- t:
			}
		}
	}
}

func tryGetFromRow[T any](row map[string]any, column string) (T, error) {
	var zero T

	v, found := row[column]

	if !found {
		return zero, fmt.Errorf("returned row has no %s column", column)
	}

	tv, ok := v.(T)

	if !ok {
		return zero, fmt.Errorf(
			"column value for %s is of the wrong type; expected %s, got %s",
			column,
			reflect.TypeOf(zero),
			reflect.TypeOf(v),
		)
	}

	return tv, nil
}
