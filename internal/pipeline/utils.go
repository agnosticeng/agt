package pipeline

import (
	"strings"

	"github.com/agnosticeng/agt/internal/utils"
)

func varsToKeyValues(m map[string]any) []any {
	var res []any

	for k, v := range m {
		lv, err := utils.ToClickHouseLiteral(v)

		if err != nil {
			res = append(res, k, v)
		} else {
			res = append(res, k, lv)
		}
	}

	return res
}

var (
	sensitiveKeywords = []string{
		"url",
		"pass",
		"secret",
		"key",
	}
)

func redactSensitiveVars(m map[string]any) map[string]any {
	var res = make(map[string]any)

	for k, v := range m {
		if containsAny(strings.ToLower(k), sensitiveKeywords) {
			res[k] = "****"
		} else {
			res[k] = v
		}
	}

	return res
}

func containsAny(s string, keywords []string) bool {
	for _, kw := range keywords {
		if strings.Contains(s, kw) {
			return true
		}
	}

	return false
}
