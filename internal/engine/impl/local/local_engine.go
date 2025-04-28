package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Masterminds/sprig"
	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/iancoleman/strcase"
	"github.com/mholt/archiver/v4"
	slogctx "github.com/veqryn/slog-context"
	"gopkg.in/yaml.v3"
)

type LocalEngineConfig struct {
	BinaryPath     string
	WorkingDir     string
	Env            map[string]string
	Bundles        []string
	BundlesPath    string
	DisableCleanup bool
	ServerSettings map[string]any
	Dsn            string
	Settings       map[string]any
	Vars           map[string]any
	Logging        *ch.LogHandlerConfig
}

type LocalEngine struct {
	conf     LocalEngineConfig
	logger   *slog.Logger
	cmd      *exec.Cmd
	connFunc func() (driver.Conn, error)
}

func NewLocalEngine(ctx context.Context, conf LocalEngineConfig) (*LocalEngine, error) {
	var logger = slogctx.FromCtx(ctx)

	if len(conf.BinaryPath) == 0 {
		conf.BinaryPath = "clickhouse"
	}

	if len(conf.BundlesPath) == 0 {
		path, err := os.UserCacheDir()

		if err != nil {
			return nil, err
		}

		conf.BundlesPath = filepath.Join(path, "agt/bundles")
	}

	for k, v := range conf.Vars {
		conf.Vars[strcase.ToScreamingSnake(k)] = v
	}

	if !filepath.IsAbs(conf.BinaryPath) {
		path, err := exec.LookPath(conf.BinaryPath)

		if err != nil {
			return nil, err
		}

		conf.BinaryPath = path
	}

	if len(conf.WorkingDir) == 0 {
		p, err := os.MkdirTemp(os.TempDir(), "*")

		if err != nil {
			return nil, err
		}

		conf.WorkingDir = p
		logger.Debug("created temporary working dir", "path", conf.WorkingDir)
	} else {
		if err := os.MkdirAll(conf.WorkingDir, 0700); err != nil {
			return nil, err
		}
	}

	if len(conf.Dsn) == 0 {
		freePort, err := findFreePort("127.0.0.1:0")

		if err != nil {
			return nil, err
		}

		conf.Dsn = fmt.Sprintf("tcp://127.0.0.1:%d/default?read_timeout=3600s", freePort)
	}

	u, err := url.Parse(conf.Dsn)

	if err != nil {
		return nil, err
	}

	logger.Info("local clickhouse server network config", "hostname", u.Hostname(), "port", u.Port())

	var finalSettings = make(map[string]interface{})
	maps.Copy(finalSettings, generateDefaultSettings(u))
	maps.Copy(finalSettings, ch.NormalizeSettings(conf.ServerSettings))

	data, err := yaml.Marshal(finalSettings)

	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(filepath.Join(conf.WorkingDir, "config.yaml"), data, 0644); err != nil {
		return nil, err
	}

	if len(conf.Bundles) > 0 {
		if err := os.MkdirAll(conf.BundlesPath, 0700); err != nil {
			return nil, err
		}

		for _, remote := range conf.Bundles {
			var local = filepath.Join(conf.BundlesPath, utils.SHA256Sum(remote))

			t, err := template.New("").Funcs(sprig.FuncMap()).Parse(remote)

			if err != nil {
				return nil, err
			}

			remotePath, err := utils.RenderTemplate(t, "", conf.Vars)

			if err != nil {
				return nil, err
			}

			logger.Debug("downloading bundle", "url", remotePath, "path", local)

			if err := utils.CachedDownload(ctx, remotePath, local); err != nil {
				return nil, fmt.Errorf("error while downloading bundle %s: %w", remote, err)
			}

			f, err := os.Open(local)

			if err != nil {
				return nil, err
			}

			format, r, err := archiver.Identify(ctx, local, f)

			if err != nil {
				return nil, err
			}

			if ex, ok := format.(archiver.Extractor); ok {
				logger.Debug("extracting bundle", "path", local)

				if err := ex.Extract(ctx, r, extractBundle(conf.WorkingDir)); err != nil {
					return nil, err
				}
			}
		}
	}

	var cmd = exec.Command(
		conf.BinaryPath,
		"server",
		"--config-file=config.yaml",
		"--log-file=clickhouse-server.log",
		"--errorlog-file=clickhouse-server-error.log",
	)

	cmd.Dir = conf.WorkingDir
	cmd.Env = slices.Clone(os.Environ())

	for k, v := range conf.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", strcase.ToScreamingSnake(k), v))
	}

	connFunc := sync.OnceValues(func() (driver.Conn, error) {
		chopts, err := clickhouse.ParseDSN(conf.Dsn)

		if err != nil {
			return nil, err
		}

		chopts.Settings = clickhouse.Settings(ch.NormalizeSettings(conf.Settings))
		chconn, err := clickhouse.Open(chopts)

		if err != nil {
			return nil, err
		}

		return chconn, nil
	})

	return &LocalEngine{
		conf:     conf,
		logger:   logger,
		cmd:      cmd,
		connFunc: connFunc,
	}, nil
}

func (eng *LocalEngine) Start() error {
	eng.logger.Info("starting local clickhouse server")
	return eng.cmd.Start()
}

func (eng *LocalEngine) Stop() {
	defer func() {
		eng.logger.Info("closing connection pool")
		conn, err := eng.connFunc()

		if err == nil {
			conn.Close()
		}
	}()

	if err := eng.cleanShutdown(context.Background()); err != nil {
		eng.logger.Info("sending SIGTERM to local clickhouse server")
		eng.cmd.Process.Signal(syscall.SIGTERM)
	}
}

func (eng *LocalEngine) cleanShutdown(ctx context.Context) error {
	eng.logger.Info("sending SYSTEM SHUTDOWN query to local clickhouse server")
	conn, err := eng.connFunc()

	if err != nil {
		return err
	}

	return conn.Exec(ctx, "SYSTEM SHUTDOWN")
}

func (eng *LocalEngine) Wait() error {
	if !eng.conf.DisableCleanup {
		defer os.RemoveAll(eng.conf.WorkingDir)
	}

	eng.logger.Info("waiting for local clickhouse server to stop")
	var err = eng.cmd.Wait()

	if err == nil {
		return nil
	}

	if exitError, ok := err.(*exec.ExitError); ok {
		if exitError.ExitCode() != 143 {
			eng.logger.Error("clickhouse server error", "log", string(exitError.Stderr))
		} else {
			return nil
		}
	}

	return err
}

func (eng *LocalEngine) Ping(ctx context.Context) error {
	conn, err := eng.connFunc()

	if err != nil {
		return err
	}

	return conn.Ping(ctx)
}

func (eng *LocalEngine) Query(ctx context.Context, query string, args ...any) ([]map[string]any, *engine.QueryMetadata, error) {
	conn, err := eng.connFunc()

	if err != nil {
		return nil, nil, err
	}

	var md engine.QueryMetadata

	rows, err := conn.Query(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(ch.ProgressHandler(&md)),
			clickhouse.WithLogs(ch.LogHandler(eng.logger, *eng.conf.Logging)),
			clickhouse.WithProfileEvents(ch.ProfileEventHandler(&md)),
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

func extractBundle(basePath string) func(ctx context.Context, info archiver.FileInfo) error {
	return func(ctx context.Context, info archiver.FileInfo) error {
		var dstPath string

		switch filepath.Dir(info.NameInArchive) {
		case "/etc/clickhouse-server", "etc/clickhouse-server":
			dstPath = filepath.Join(basePath, filepath.Base(info.NameInArchive))
		case "/var/lib/clickhouse/user_defined", "var/lib/clickhouse/user_defined":
			dstPath = filepath.Join(basePath, "user_defined", filepath.Base(info.NameInArchive))
		case "/var/lib/clickhouse/user_scripts", "var/lib/clickhouse/user_scripts":
			dstPath = filepath.Join(basePath, "user_scripts", filepath.Base(info.NameInArchive))
		default:
			return nil
		}

		if err := os.MkdirAll(filepath.Dir(dstPath), 0744); err != nil {
			return err
		}

		r, err := info.Open()

		if err != nil {
			return err
		}

		defer r.Close()

		w, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE, info.FileInfo.Mode())

		if err != nil {
			return err
		}

		if _, err := io.Copy(w, r); err != nil {
			return err
		}

		return w.Close()
	}
}

func findFreePort(host string) (int, error) {
	var addr, err = net.ResolveTCPAddr("tcp", host)

	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)

	if err != nil {
		return 0, err
	}

	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func generateDefaultSettings(dsn *url.URL) clickhouse.Settings {
	var settings = make(clickhouse.Settings)

	settings["path"] = "./"
	settings["user_defined_executable_functions_config"] = "*_function.*ml"
	settings["listen_host"] = dsn.Hostname()
	settings["tcp_port"] = dsn.Port()
	settings["profiles"] = map[string]any{
		"default": map[string]any{},
	}
	settings["users"] = map[string]any{
		"default": map[string]any{
			"password":                 "",
			"access_management":        1,
			"named_collection_control": 1,
		},
	}
	settings["shutdown_wait_unfinished_queries"] = 0
	settings["cache_size_to_ram_max_ratio"] = 0.1
	settings["cgroup_memory_watcher_soft_limit_ratio"] = 0.25

	// Low-memory server settings taken from here: https://kb.altinity.com/altinity-kb-setup-and-maintenance/configure_clickhouse_for_low_mem_envs/
	settings["mysql_port"] = map[string]any{"@remove": "remove"}
	settings["postgresql_port"] = map[string]any{"@remove": "remove"}
	settings["query_thread_log"] = map[string]any{"@remove": "remove"}
	settings["opentelemetry_span_log"] = map[string]any{"@remove": "remove"}
	settings["processors_profile_log"] = map[string]any{"@remove": "remove"}
	settings["asynchronous_metric_log"] = map[string]any{"@remove": "remove"}
	settings["backup_log"] = map[string]any{"@remove": "remove"}
	settings["metric_log"] = map[string]any{"@remove": "remove"}
	settings["query_log"] = map[string]any{"@remove": "remove"}
	settings["query_views_log"] = map[string]any{"@remove": "remove"}
	settings["part_log"] = map[string]any{"@remove": "remove"}
	settings["session_log"] = map[string]any{"@remove": "remove"}
	settings["text_log"] = map[string]any{"@remove": "remove"}
	settings["trace_log"] = map[string]any{"@remove": "remove"}
	settings["zookeeper_log"] = map[string]any{"@remove": "remove"}
	settings["mlock_executable"] = false
	settings["mark_cache_size"] = 268435456
	settings["index_mark_cache_size"] = 67108864
	settings["uncompressed_cache_size"] = 16777216
	settings["max_thread_pool_size"] = 2000
	settings["max_server_memory_usage_to_ram_ratio"] = 0.75
	settings["max_server_memory_usage"] = 0
	settings["background_pool_size"] = 2
	settings["background_merges_mutations_concurrency_ratio"] = 2
	settings["merge_tree"] = map[string]any{
		"merge_max_block_size":                                                4096,
		"max_bytes_to_merge_at_max_space_in_pool":                             1073741824,
		"number_of_free_entries_in_pool_to_lower_max_size_of_merge":           2,
		"number_of_free_entries_in_pool_to_execute_mutation":                  2,
		"number_of_free_entries_in_pool_to_execute_optimize_entire_partition": 2,
	}
	settings["background_buffer_flush_schedule_pool_size"] = 1
	settings["background_merges_mutations_scheduling_policy"] = "round_robin"
	settings["background_move_pool_size"] = 1
	settings["background_fetches_pool_size"] = 1
	settings["background_common_pool_size"] = 2
	settings["background_schedule_pool_size"] = 2
	settings["background_message_broker_schedule_pool_size"] = 0
	settings["background_distributed_schedule_pool_size"] = 0

	return settings
}
