#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif
   void knn_L2sqr(
           const float* x,
           const float* y,
           size_t d,
           size_t nx,
           size_t ny,
           size_t k,
           float* distances,
           int64_t* indexes,
           const float* y_norm2);

#ifdef __cplusplus
}
#endif