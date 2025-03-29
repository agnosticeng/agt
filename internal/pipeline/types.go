package pipeline

import (
	"fmt"
	"sort"
	"strconv"
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
