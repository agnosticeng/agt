package ch

import (
	"time"

	"github.com/uber-go/tally/v4"
)

type QueryMetrics struct {
	ExecutionTime tally.Histogram
	Elapsed       tally.Histogram
	Rows          tally.Counter
	Bytes         tally.Counter
	TotalRows     tally.Counter
	WroteRows     tally.Counter
	WroteBytes    tally.Counter
}

func NewQueryMetrics(scope tally.Scope) *QueryMetrics {
	return &QueryMetrics{
		ExecutionTime: scope.Histogram(
			"execution_time",
			tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 2, 10),
		),
		Elapsed: scope.Histogram(
			"elapsed",
			tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 2, 10),
		),
		Rows:       scope.Counter("rows"),
		Bytes:      scope.Counter("bytes"),
		TotalRows:  scope.Counter("total_rows"),
		WroteRows:  scope.Counter("wrote_rows"),
		WroteBytes: scope.Counter("wrote_bytes"),
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
