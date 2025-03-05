package utils

import (
	"bytes"
	"context"
	"net/url"
	"path/filepath"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/utils"
)

func RenderTemplate(tmpl *template.Template, name string, vars map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	if err := tmpl.ExecuteTemplate(&buf, name, vars); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func LoadTemplates(ctx context.Context, target *url.URL) (*template.Template, error) {
	var (
		os   = objstr.FromContextOrDefault(ctx)
		tmpl = template.New("pipeline").Option("missingkey=default").Funcs(sprig.FuncMap())
	)

	files, err := os.ListPrefix(ctx, target)

	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if filepath.Ext(file.URL.Path) != ".sql" {
			continue
		}

		content, err := utils.ReadObject(ctx, os, file.URL)

		if err != nil {
			return nil, err
		}

		if _, err := tmpl.New(filepath.Base(file.URL.Path)).Parse(string(content)); err != nil {
			return nil, err
		}
	}

	return tmpl, nil
}
