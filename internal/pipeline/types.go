package pipeline

import (
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"

	"github.com/mitchellh/mapstructure"
)

type Task struct {
	SequenceNumberStart int64
	SequenceNumberEnd   int64
	Vars                map[string]any
}

func (t Task) Id() string {
	if t.SequenceNumberStart == t.SequenceNumberEnd {
		return strconv.FormatInt(t.SequenceNumberStart, 10)
	}

	return fmt.Sprintf("%d_%d", t.SequenceNumberStart, t.SequenceNumberEnd)
}

type Tasks []*Task

func (ts *Tasks) Insert(t *Task) {
	var i = sort.Search(len(*ts), func(i int) bool {
		return (*ts)[i].SequenceNumberStart > t.SequenceNumberStart
	})

	*ts = append(*ts, nil)
	copy((*ts)[i+1:], (*ts)[i:])
	(*ts)[i] = t
}

type QueryFile struct {
	Path          string
	IgnoreFailure bool
}

func ParseQueryFile(s string) (*QueryFile, error) {
	var res QueryFile
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

	u.Fragment = ""
	u.RawFragment = ""
	res.Path = u.String()
	return &res, nil
}

func StringToQueryFileHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if (f.Kind() != reflect.String) || (t != reflect.TypeOf(QueryFile{})) {
			return data, nil
		}

		qf, err := ParseQueryFile(data.(string))

		if err != nil {
			return nil, err
		}

		return qf, nil
	}
}
