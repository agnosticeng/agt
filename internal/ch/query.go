package ch

import (
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func IsDataQuery(query string) bool {
	query = strings.ToUpper(query)
	query = strings.TrimLeft(query, " ")

	switch {
	case strings.HasPrefix(query, "SELECT"):
		return true
	case strings.HasPrefix(query, "DESCRIBE"):
		return true
	default:
		return false
	}
}

func RowsToMaps(rows driver.Rows) ([]map[string]interface{}, error) {
	var (
		columnNames = rows.Columns()
		columnTypes = rows.ColumnTypes()
		res         []map[string]any
	)

	for rows.Next() {
		var (
			rowData = make([]any, len(columnTypes))
			item    = make(map[string]any)
		)

		for i := range columnTypes {
			rowData[i] = reflect.New(columnTypes[i].ScanType()).Interface()
		}

		if err := rows.Scan(rowData...); err != nil {
			return nil, err
		}

		for i, col := range rowData {
			if !strings.HasPrefix(columnNames[i], "_") {
				item[columnNames[i]] = col
			}
		}

		res = append(res, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
