package flex

import "iter"

type Iter[V any] interface {
	Range() iter.Seq[V]
	// Error reports any error that caused the returned sequence to stop
	// iterating. Always call Error() after iteration finishes.
	Error() error
}

type Iter2[K, V any] interface {
	Range() iter.Seq2[K, V]
	// Error reports any error that caused the returned sequence to stop
	// iterating. Always call Error() after iteration finishes.
	Error() error
}

type iterator[V any] struct {
	seq func(func(V) bool) error
	err error
}

func NewIter[V any](seq func(yield func(V) bool) error) Iter[V] {
	return &iterator[V]{seq: seq}
}

func (it *iterator[V]) Range() iter.Seq[V] {
	return func(yield func(V) bool) {
		it.err = it.seq(yield)
	}
}

func (it *iterator[V]) Error() error {
	return it.err
}

type iterator2[K, V any] struct {
	seq func(func(K, V) bool) error
	err error
}

func NewIter2[K, V any](seq func(yield func(K, V) bool) error) Iter2[K, V] {
	return &iterator2[K, V]{seq: seq}
}

func (it *iterator2[K, V]) Range() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		it.err = it.seq(yield)
	}
}

func (it *iterator2[K, V]) Error() error {
	return it.err
}
