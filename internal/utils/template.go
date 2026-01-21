package utils

import (
	"bytes"
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/utils"
	"github.com/samber/lo"
)

func RenderTemplate(tmpl *template.Template, name string, vars map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	if err := tmpl.ExecuteTemplate(&buf, name, vars); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func LoadTemplates(ctx context.Context, basePath string, includes []string) (*template.Template, error) {
	var (
		os   = objstr.FromContextOrDefault(ctx)
		tmpl = template.New("pipeline").
			Option("missingkey=default").
			Funcs(lo.Assign(
				sprig.FuncMap(),
				FuncMap(),
			))
	)

	var templatesUrls = make([]*url.URL, 0, len(includes)+1)
	baseUrl, err := getBaseUrl(basePath)
	if err != nil {
		return nil, err
	}

	templatesUrls = append(templatesUrls, baseUrl)

	for _, include := range includes {
		u, err := url.Parse(include)
		if err != nil {
			return nil, err
		}

		if len(u.Scheme) > 0 {
			templatesUrls = append(templatesUrls, u)
			continue
		}

		if strings.HasPrefix(u.Path, "/") {
			templatesUrls = append(templatesUrls, u)
			continue
		}

		absUrl, err := url.Parse(baseUrl.String())
		if err != nil {
			return nil, err
		}

		absUrl.Path = filepath.Join(baseUrl.Path, u.Path)
		templatesUrls = append(templatesUrls, absUrl)

	}

	for _, u := range templatesUrls {
		files, err := os.ListPrefix(ctx, u)

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
	}

	return tmpl, nil
}

func getBaseUrl(s string) (*url.URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	path, err := filepath.Abs(u.Path)
	if err != nil {
		return nil, err
	}

	u.Path = filepath.Dir(path)

	if len(u.Scheme) == 0 {
		u.Scheme = "file"
	}

	return u, nil
}
