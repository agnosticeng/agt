package pipeline

import "github.com/agnosticeng/agt/internal/utils"

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
