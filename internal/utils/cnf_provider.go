package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/agnosticeng/objstr"
	objstrutils "github.com/agnosticeng/objstr/utils"
	"github.com/samber/lo"
	"gopkg.in/yaml.v2"
)

type CnfProvider struct {
	os   *objstr.ObjectStore
	path string
	vars map[string]any
}

func NewCnfProvider(os *objstr.ObjectStore, path string, vars map[string]any) *CnfProvider {
	return &CnfProvider{os: os, path: path, vars: vars}
}

func (p *CnfProvider) ReadMap() (map[string]interface{}, error) {
	u, err := url.Parse(p.path)
	if err != nil {
		return nil, err
	}

	content, err := objstrutils.ReadObject(context.Background(), p.os, u)
	if err != nil {
		return nil, err
	}

	tmpl, err := template.New("pipeline").
		Option("missingkey=default").
		Funcs(lo.Assign(
			sprig.FuncMap(),
			FuncMap(),
		)).Parse(string(content))

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, p.vars); err != nil {
		return nil, err
	}

	var unmarshaler func([]byte, interface{}) error

	switch filepath.Ext(u.Path) {
	case ".json":
		unmarshaler = json.Unmarshal
	case ".yaml", ".yml":
		unmarshaler = yaml.Unmarshal
	default:
		return nil, fmt.Errorf("unhandled file extension: %s", filepath.Ext(p.path))
	}

	var m map[string]interface{}
	if err := unmarshaler(buf.Bytes(), &m); err != nil {
		return nil, err
	}

	return m, nil
}
