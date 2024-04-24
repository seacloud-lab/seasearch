package zutils

import (
	"encoding/binary"
	"math"
)

// VectorToBytes
// Convert vector to bytes for storage.
// It's much faster than json marshal.
func VectorToBytes(vector []float32) []byte {
	bts := make([]byte, len(vector)*4)
	for i, x := range vector {
		binary.LittleEndian.PutUint32(bts[4*i:], math.Float32bits(x))
	}
	return bts
}

// BytesToVector
// Convert bytes to vector.
// It's much faster than json unmarshal
func BytesToVector(bts []byte) []float32 {
	dataSize := len(bts) / 4
	res := make([]float32, dataSize)
	for i := range res {
		res[i] = math.Float32frombits(binary.LittleEndian.Uint32(bts[4*i:]))
	}
	return res
}
