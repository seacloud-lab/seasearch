package zutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/x448/float16"
)

func TestFloat16Bytes(t *testing.T) {
	q := []float32{0.37454011, 0.95071430640, 0.731993941, 0.5986584841}
	qAfter16 := make([]float32, 4)
	for i := range q {
		qAfter16[i] = float16.Fromfloat32(q[i]).Float32()
	}
	bts := VectorToFloat16Bytes(q)

	q16f32 := Float16BytesToVector(bts)
	assert.Equal(t, q16f32, qAfter16)
}

func TestFloat16(t *testing.T) {
	// These numbers can be accurately represented in floating,
	// and we use them to verify the correctness of float16 serialization.
	q := []float32{0.625, 0.125, 0.75, 0.5}
	bts := VectorToFloat16Bytes(q)
	assert.Equal(t, len(q)*2, len(bts))
	q16f32 := Float16BytesToVector(bts)
	assert.Equal(t, q16f32, q)
}
