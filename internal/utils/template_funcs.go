package utils

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"time"
)

func FuncMap() template.FuncMap {
	return template.FuncMap{
		"deref":               Deref,
		"toCH":                ToClickHouseLiteral,
		"toClickHouseLiteral": ToClickHouseLiteral,
	}
}

func Deref(v any) (any, error) {
	var rv = reflect.ValueOf(v)

	if rv.Kind() == reflect.Pointer {
		return rv.Elem().Interface(), nil
	}

	return v, nil
}

func ToClickHouseLiteral(v any) (string, error) {
	var rv = reflect.ValueOf(v)

	if !rv.IsValid() || rv.IsZero() {
		return "null", nil
	}

	switch rv.Kind() {
	case reflect.Pointer:
		return ToClickHouseLiteral(rv.Elem().Interface())
	case reflect.String:
		return "'" + rv.String() + "'", nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10), nil

	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", rv.Float()), nil

	case reflect.Slice:
		var b strings.Builder
		b.WriteString("[")

		for i := 0; i < rv.Len(); i++ {
			s, err := ToClickHouseLiteral(rv.Index(i).Interface())

			if err != nil {
				return "", err
			}

			b.WriteString(s)

			if i < (rv.Len() - 1) {
				b.WriteString(",")
			}
		}

		b.WriteString("]")
		return b.String(), nil

	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return "", fmt.Errorf("map must have string key (have %s)", rv.Type().Key().Kind())
		}

		var b strings.Builder
		b.WriteString("map(")

		for i, k := range rv.MapKeys() {
			key, err := ToClickHouseLiteral(k.Interface())

			if err != nil {
				return "", err
			}

			value, err := ToClickHouseLiteral(rv.MapIndex(k).Interface())

			if err != nil {
				return "", err
			}

			b.WriteString(key)
			b.WriteString(",")
			b.WriteString(value)

			if i < (len(rv.MapKeys()) - 1) {
				b.WriteString(",")
			}
		}

		b.WriteString(")")
		return b.String(), nil

	case reflect.Struct:
		switch {
		case rv.Type() == reflect.TypeOf(time.Time{}):
			return "toDateTime('" + (rv.Interface().(time.Time)).UTC().Format(time.DateTime) + "', 'UTC')", nil
		default:
			return "", fmt.Errorf("unhandled type: %s", rv.Type().String())
		}

	default:
		return "", fmt.Errorf("unhandled type: %s", rv.Type().String())
	}
}
