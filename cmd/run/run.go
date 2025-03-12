package run

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine/impl"
	"github.com/agnosticeng/agt/internal/pipeline"
	"github.com/agnosticeng/agt/internal/utils"
	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/cnf/providers/env"
	"github.com/agnosticeng/objstr"
	objstrutils "github.com/agnosticeng/objstr/utils"
	"github.com/agnosticeng/panicsafe"
	"github.com/agnosticeng/tallyctx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
	"golang.org/x/sync/errgroup"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "template-path"},
	&cli.StringSliceFlag{Name: "var"},
}

type config struct {
	pipeline.PipelineConfig
	Engine       impl.EngineConfig
	StartupProbe ch.StartupProbeConfig
	PromAddr     string
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				logger               = slogctx.FromCtx(ctx.Context)
				path                 = ctx.Args().Get(0)
				templatePath         = ctx.String("template-path")
				vars                 = utils.ParseKeyValues(ctx.StringSlice("var"), "=")
				cfg                  config
				sigCtx, sigCtxCancel = signal.NotifyContext(ctx.Context, os.Interrupt, syscall.SIGTERM)
			)

			defer sigCtxCancel()

			if len(path) == 0 {
				return fmt.Errorf("pipeline path must be specified")
			}

			if err := cnf.Load(
				&cfg,
				cnf.WithProvider(objstrutils.NewCnfProvider(objstr.FromContextOrDefault(sigCtx), path)),
				cnf.WithProvider(env.NewEnvProvider("AGN")),
				cnf.WithMapstructureHooks(pipeline.StringToQueryFileHookFunc()),
			); err != nil {
				return err
			}

			u, err := url.Parse(path)

			if err != nil {
				return err
			}

			if len(templatePath) == 0 {
				u.Path = filepath.Dir(u.Path)
			}

			tmpl, err := utils.LoadTemplates(sigCtx, u)

			if err != nil {
				return err
			}

			var pipelineCtx, pipelineCancel = signal.NotifyContext(sigCtx, syscall.SIGTERM)
			defer pipelineCancel()

			var promReporter = promreporter.NewReporter(promreporter.Options{
				OnRegisterError: func(err error) {
					logger.Log(sigCtx, -30, "failed to register metric", "error", err.Error())
				},
			})

			scope, scopeCloser := tally.NewRootScope(tally.ScopeOptions{
				Prefix:         "agnostic_etl_engine",
				CachedReporter: promReporter,
				Separator:      promreporter.DefaultSeparator,
			}, 1*time.Second)

			defer scopeCloser.Close()

			if len(cfg.PromAddr) == 0 {
				cfg.PromAddr = ":9999"
			}

			go func() {
				logger.Info("prometheus HTTP server started", "addr", cfg.PromAddr)
				http.ListenAndServe(cfg.PromAddr, promhttp.Handler())
			}()

			pipelineCtx = tallyctx.NewContext(pipelineCtx, scope)

			engine, err := impl.NewEngine(pipelineCtx, cfg.Engine)

			if err != nil {
				return err
			}

			if err := engine.Start(); err != nil {
				return err
			}

			var group, groupCtx = errgroup.WithContext(pipelineCtx)

			group.Go(panicsafe.Func(func() error {
				var err = engine.Wait()

				if err != nil {
					logger.Error("engine stopped", "error", err.Error())
				} else {
					logger.Info("engine stopped")
				}

				return err
			}))

			group.Go(panicsafe.Func(func() error {
				defer engine.Stop()

				if err := ch.RunStartupProbe(groupCtx, engine, cfg.StartupProbe); err != nil {
					return err
				}

				return pipeline.Run(
					groupCtx,
					engine,
					tmpl,
					vars,
					cfg.PipelineConfig,
				)
			}))

			return group.Wait()
		},
	}
}
