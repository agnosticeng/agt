package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func ParseKeyValues(kvs []string, separator string) map[string]interface{} {
	var m = make(map[string]interface{})

	for _, kv := range kvs {
		var k, v, _ = strings.Cut(kv, separator)
		m[k] = v
	}

	return m
}

func ParseKeyValuesWithPrefix(input []string, separator string, prefix string) map[string]interface{} {
	var (
		res = make(map[string]interface{})
		kvs = ParseKeyValues(input, separator)
	)

	for k, v := range kvs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}

		res[strings.TrimPrefix(k, prefix)] = v
	}

	return res
}

func SHA256Sum(s string) string {
	var h = sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}
