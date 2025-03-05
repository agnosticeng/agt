package utils

import "maps"

func MergeMaps[K comparable, V any](ms ...map[K]V) map[K]V {
	var res = make(map[K]V)

	for _, m := range ms {
		maps.Copy(res, m)
	}

	return res
}
