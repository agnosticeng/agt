package remote

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
	slogctx "github.com/veqryn/slog-context"
)

type RemoteEngineConfig struct {
	Dsn      string
	Settings map[string]any
}

type RemoteEngine struct {
	conf     RemoteEngineConfig
	logger   *slog.Logger
	stopChan chan interface{}
	conn     driver.Conn
}

func NewRemoteEngine(ctx context.Context, conf RemoteEngineConfig) (*RemoteEngine, error) {
	chopts, err := clickhouse.ParseDSN(conf.Dsn)

	if err != nil {
		return nil, err
	}

	chopts.Settings = clickhouse.Settings(ch.NormalizeSettings(conf.Settings))
	chconn, err := clickhouse.Open(chopts)

	if err != nil {
		return nil, err
	}

	return &RemoteEngine{
		conf:     conf,
		logger:   slogctx.FromCtx(ctx),
		stopChan: make(chan interface{}, 1),
		conn:     chconn,
	}, nil
}

func (eng *RemoteEngine) Start() error {
	return nil
}

func (eng *RemoteEngine) Stop() {
	eng.conn.Close()
	close(eng.stopChan)
}

func (eng *RemoteEngine) Wait() error {
	<-eng.stopChan
	return nil
}

func (eng *RemoteEngine) Ping(ctx context.Context) error {
	return eng.conn.Ping(ctx)
}

func (eng *RemoteEngine) Query(ctx context.Context, query string, args ...any) ([]map[string]any, *engine.QueryMetadata, error) {
	var md engine.QueryMetadata

	rows, err := eng.conn.Query(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(ch.ProgressHandler(&md)),
			clickhouse.WithLogs(ch.LogHandler(eng.logger)),
		),
		query,
		args...,
	)

	if errors.Is(err, io.EOF) && !ch.IsDataQuery(query) {
		return nil, &md, nil
	}

	if err != nil {
		return nil, &md, err
	}

	res, err := ch.RowsToMaps(rows)
	return res, &md, err
}
