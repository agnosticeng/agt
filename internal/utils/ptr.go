package utils

func Ptr[T any](t T) *T {
	return &t
}

func LastElemOrZero[T any](elems []T) T {
	var zero T

	if len(elems) == 0 {
		return zero
	}

	return elems[len(elems)-1]
}

func LastElemOrDefault[T any](elems []T, _default T) T {
	if len(elems) == 0 {
		return _default
	}

	return elems[len(elems)-1]
}
