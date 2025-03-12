package pipeline

import (
	"context"
	"text/template"

	"github.com/agnosticeng/agt/internal/ch"
	"github.com/agnosticeng/agt/internal/engine"
)

type InitConfig struct {
	Queries []QueryFile
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
) (map[string]any, error) {
	for i, query := range conf.Queries {
		rows, _, err := ch.QueryFromTemplate(ctx, engine, tmpl, query.Path, vars)

		if err != nil && !query.IgnoreFailure {
			return nil, err
		}

		if i == (len(conf.Queries)-1) && len(rows) > 0 {
			return rows[len(rows)-1], nil
		}

	}

	return nil, nil
}
