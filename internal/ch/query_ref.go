package ch

import (
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"github.com/uber-go/tally/v4"
)

type QueryRef struct {
	Name             string
	IgnoreFailure    bool
	IgnoreErrorCodes []int
}

func (ref *QueryRef) Metrics(scope tally.Scope) *QueryMetrics {
	if ref == nil {
		return nil
	}

	return NewQueryMetrics(scope.Tagged(map[string]string{"query": ref.Name}))
}

func ParseQueryRef(s string) (*QueryRef, error) {
	var res QueryRef
	u, err := url.Parse(s)

	if err != nil {
		return nil, err
	}

	q, err := url.ParseQuery(u.Fragment)

	if err != nil {
		return nil, err
	}

	if v := q.Get("ignore-failure"); len(v) > 0 {
		if b, err := strconv.ParseBool(v); err == nil {
			res.IgnoreFailure = b
		}
	}

	if v := q.Get("ignore-error-codes"); len(v) > 0 {
		var codes = lo.Compact(strings.Split(v, ","))

		for _, code := range codes {
			if c, err := strconv.ParseInt(code, 10, 64); err == nil {
				res.IgnoreErrorCodes = append(res.IgnoreErrorCodes, int(c))
			}
		}
	}

	u.Fragment = ""
	u.RawFragment = ""
	res.Name = u.String()
	return &res, nil
}

func StringToQueryRefHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if (f.Kind() != reflect.String) || (t != reflect.TypeOf(QueryRef{})) {
			return data, nil
		}

		qf, err := ParseQueryRef(data.(string))

		if err != nil {
			return nil, err
		}

		return qf, nil
	}
}
