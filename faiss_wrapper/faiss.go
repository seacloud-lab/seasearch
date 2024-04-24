package faiss_wrapper

/*

#cgo LDFLAGS: -lfaiss
#cgo CXXFLAGS: -std=c++11

#include "./faiss_wrapper.h"
*/
import "C"

func Knn(query []float32, base []float32, nq, nb, k int, dim int) ([]float32, []int64) {
	distances := make([]float32, k)
	indexes := make([]int64, k)
	C.knn_L2sqr(
		(*C.float)(&query[0]),
		(*C.float)(&base[0]),
		(C.size_t)(dim),
		(C.size_t)(nq),
		(C.size_t)(nb),
		(C.size_t)(k),
		(*C.float)(&distances[0]),
		(*C.int64_t)(&indexes[0]),
		(*C.float)(nil))
	return distances, indexes
}
