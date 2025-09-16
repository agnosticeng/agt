package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/agnosticeng/agt/cmd/render"
	"github.com/agnosticeng/agt/cmd/run"
	"github.com/agnosticeng/cliutils"
	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/cnf/providers/env"
	objstrcli "github.com/agnosticeng/objstr/cli"
	"github.com/agnosticeng/panicsafe"
	"github.com/agnosticeng/slogcli"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "agt",
		Flags: slogcli.SlogFlags(),
		Before: cliutils.CombineBeforeFuncs(
			slogcli.SlogBefore,
			objstrcli.ObjStrBefore(cnf.WithProvider(env.NewEnvProvider("OBJSTR"))),
		),
		After: cliutils.CombineAfterFuncs(
			objstrcli.ObjStrAfter,
			slogcli.SlogAfter,
		),
		Commands: []*cli.Command{
			run.Command(),
			render.Command(),
		},
	}

	var err = panicsafe.Recover(func() error { return app.Run(os.Args) })

	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(1)
	}
}
