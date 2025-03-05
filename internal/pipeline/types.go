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

// var t0 = Task{
// 	SequenceNumberStart: 0,
// 	SequenceNumberEnd:   0,
// 	Vars: map[string]any{
// 		"RANGE_START": 0,
// 		"RANGE_END":   9,
// 		"BUFFER": "buf_0_9",
// 	},
// }

// var t1 = Task{
// 	SequenceNumberStart: 1,
// 	SequenceNumberEnd:   1,
// 	Vars: map[string]any{
// 		"RANGE_START": 10,
// 		"RANGE_END":   19,
// 		"BUFFER": "buf_10_19",
// 	},
// }

// select
// 	least([{{.LEFT.RANGE_START}}, {{.RIGHT.RANGE_START}}]) as RANGE_START,
// 	greatest([{{.LEFT.RANGE_END}}, {{.RIGHT.RANGE_END}}]) AS RANGE_END,
// 	arrayConcat({{.LEFT.BUFFER}}, {{.RIGHT.BUFFER}})

// var t0_1 = Task{
// 	SequenceNumberStart: 0,
// 	SequenceNumberEnd:   1,
// 	Vars: map[string]any{
// 		"RANGE_START": 0,
// 		"RANGE_END":   19,
// 		"BUFFERS": ["buf_0_9", "buf_10_19"]
// 	},
// }
