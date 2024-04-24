#include "./faiss_wrapper.h"
#include <stdlib.h>
#include <faiss/utils/distances.h>

void knn_L2sqr(
           const float* x,
           const float* y,
           size_t d,
           size_t nx,
           size_t ny,
           size_t k,
           float* distances,
           int64_t* indexes,
           const float* y_norm2){
            return faiss::knn_L2sqr(x,y,d,nx,ny,k,distances,indexes,y_norm2,NULL);
       }
