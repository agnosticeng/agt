package ch

import (
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func NormalizeSettings(settings clickhouse.Settings) clickhouse.Settings {
	var m = make(clickhouse.Settings)

	for k, v := range settings {
		m[strings.ToLower(k)] = v
	}

	return m
}
