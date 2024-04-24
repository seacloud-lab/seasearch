package faiss_wrapper

import (
	"math/rand"
	"testing"
)

func TestFaissKnn(t *testing.T) {
	d := 64      // dimension
	nb := 100000 // database size
	nq := 1

	xb := make([]float32, d*nb)
	xq := make([]float32, d*nq)

	for i := 0; i < nb; i++ {
		for j := 0; j < d; j++ {
			xb[i*d+j] = rand.Float32()
		}
		xb[i*d] += float32(i) / 1000
	}
	for i := 0; i < nq; i++ {
		for j := 0; j < d; j++ {
			xq[i*d+j] = rand.Float32()
		}
		xq[i*d] += float32(i) / 1000
	}
	k := 10

	distances, ids := Knn(xq, xb, nq, nb, k, d)
	for i := 0; i < k; i++ {
		t.Logf("d %.2f, i %d ", distances[i], ids[i])
	}
}
