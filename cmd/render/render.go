package render

import (
	"fmt"
	"path/filepath"

	"github.com/agnosticeng/agt/internal/utils"
	"github.com/urfave/cli/v2"
)

var Flags = []cli.Flag{
	&cli.StringSliceFlag{Name: "var"},
	&cli.StringFlag{Name: "filter"},
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "render",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				path   = ctx.Args().Get(0)
				vars   = utils.ParseKeyValues(ctx.StringSlice("var"), "=")
				filter = ctx.String("filter")
			)

			tmpl, err := utils.LoadTemplates(ctx.Context, path, nil)

			if err != nil {
				return err
			}

			for _, tmpl := range tmpl.Templates() {
				if len(filter) > 0 {
					if m, _ := filepath.Match(filter, tmpl.Name()); !m {
						continue
					}
				}

				fmt.Println("--------------------------------------------------------------------------------")
				fmt.Println(tmpl.Name())
				fmt.Println("--------------------------------------------------------------------------------")

				str, err := utils.RenderTemplate(tmpl, tmpl.Name(), vars)

				if err != nil {
					return err
				}

				fmt.Println(str)
			}

			return nil
		},
	}
}
