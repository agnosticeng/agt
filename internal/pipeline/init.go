package pipeline

import (
	"context"
	"text/template"

	"github.com/agnosticeng/agnostic-etl-engine/internal/ch"
	"github.com/agnosticeng/agnostic-etl-engine/internal/engine"
)

type InitConfig struct {
	Queries []string
}

func (conf InitConfig) WithDefaults() InitConfig {
	return conf
}

func Init(
	ctx context.Context,
	engine engine.Engine,
	tmpl *template.Template,
	vars map[string]interface{},
	conf InitConfig,
) error {
	for _, query := range conf.Queries {
		_, _, err := ch.QueryFromTemplate(ctx, engine, tmpl, query, vars)

		if err != nil {
			return err
		}
	}

	return nil
}
