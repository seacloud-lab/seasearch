package vector

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/x448/float16"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	vectorstore "github.com/zincsearch/zincsearch/pkg/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
)

func configureVectorTest(t *testing.T) {
	t.Helper()
	original := *config.Global
	t.Cleanup(func() { *config.Global = original })
	t.Setenv("SS_DATA_PATH", t.TempDir())
	t.Setenv("SS_STORAGE_TYPE", "disk")
	config.InitConfig()
}

// Migrated from core's Flat/IVFPQ/HNSW regressions. Metadata is fixture-owned
// and explicitly borrowed on each call, rather than stored inside the runtime.
func TestVecIndex(t *testing.T) {
	for _, kind := range []string{vectorstore.Flat, vectorstore.IVFPQ, vectorstore.HNSW} {
		t.Run(kind, func(t *testing.T) {
			configureVectorTest(t)
			ctx := context.Background()
			dims := 4
			if kind == vectorstore.HNSW {
				dims = 2
			}
			ref := &meta.VecIndex{
				TargetType: kind, Dims: dims, M: 2, NBits: 4,
				Segments: []*meta.VectorSegment{{Id: 0, Status: vectorstore.StatusGrowing}},
			}
			options := Options{Name: "test", StoreName: "test", StorageType: "disk"}
			manager := NewVecIndexManager(options)
			defer func() { manager.Close() }()
			idx, err := manager.Get("field", ref)
			require.NoError(t, err)
			require.NoError(t, idx.ReconcileLayout(ctx, ref))

			vectors := [][]float32{
				{0.1, 0.2, 0.3, 0.4}, {0.1, 0.3, 0.2, 0.3},
				{0.1, 0.2, 0.6, 0.4}, {0.1, 0.2, 0.3, 0.9},
			}
			query := []float32{0.1, 0.1, 0.2, 0.3}
			if kind == vectorstore.HNSW {
				for i := range vectors {
					vectors[i] = zutils.MinuteVector(i + 1)
				}
				query = zutils.MinuteVector(0)
			}
			_, err = idx.Batch(ctx, ref, vectors, []int64{1, 2, 3, 4}, nil)
			require.NoError(t, err)
			require.EqualValues(t, 4, ref.Count)
			require.EqualValues(t, 4, ref.Segments[0].Count)
			result, err := idx.Search(ctx, ref, query, 2, 10)
			require.NoError(t, err)
			require.Len(t, result, 2)
			for _, id := range []int64{1, 2} {
				require.Contains(t, result, base62.Encode(id))
				if kind != vectorstore.HNSW {
					distance, err := L2Distance(vectors[id-1], query)
					require.NoError(t, err)
					require.InDelta(t, distance, result[base62.Encode(id)], 1e-7)
				}
			}
			filtered, err := idx.SearchByIDs(ctx, ref, query, 2, []string{base62.Encode(2), base62.Encode(3)})
			require.NoError(t, err)
			require.Len(t, filtered, 2)
			require.Equal(t, base62.Encode(2), filtered[0].DocID)
			require.Equal(t, base62.Encode(3), filtered[1].DocID)
			if kind == vectorstore.HNSW {
				require.NoError(t, idx.BuildHNSW(ctx, ref))
			}
			_, err = idx.Batch(ctx, ref, nil, nil, []int64{1})
			require.NoError(t, err)
			require.EqualValues(t, 3, ref.Count)
			manager.Close()
			manager = NewVecIndexManager(options)
			idx, err = manager.Get("field", ref)
			require.NoError(t, err)
			result, err = idx.Search(ctx, ref, query, 4, 10)
			require.NoError(t, err)
			require.Len(t, result, 3)
			require.NotContains(t, result, base62.Encode(1))
		})
	}
}

func TestSearchFloat16Flat(t *testing.T) {
	configureVectorTest(t)
	ref := &meta.VecIndex{
		TargetType: vectorstore.Flat, Dims: 4, StoreWithFloat16: true,
		Segments: []*meta.VectorSegment{{Id: 0, Status: vectorstore.StatusGrowing}},
	}
	manager := NewVecIndexManager(Options{Name: "float16", StoreName: "float16", StorageType: "disk"})
	defer manager.Close()
	idx, err := manager.Get("field", ref)
	require.NoError(t, err)
	ctx := context.Background()
	vectors := [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.1, 0.3, 0.2, 0.3}}
	_, err = idx.Batch(ctx, ref, vectors, []int64{1, 2}, nil)
	require.NoError(t, err)
	query := []float32{0.1, 0.1, 0.2, 0.3}
	result, err := idx.Search(ctx, ref, query, 2, 0)
	require.NoError(t, err)
	require.Len(t, result, 2)
	for i, vec := range vectors {
		for j, value := range vec {
			vec[j] = float16.Fromfloat32(value).Float32()
		}
		distance, err := L2Distance(vec, query)
		require.NoError(t, err)
		require.InDelta(t, distance, result[base62.Encode(int64(i+1))], 1e-7)
	}
}

func TestIVFPQSealAndReload(t *testing.T) {
	t.Setenv("SS_VECTOR_IVFPQ_THRESHOLD", "5000")
	configureVectorTest(t)
	ctx := context.Background()
	ref := &meta.VecIndex{
		TargetType: vectorstore.IVFPQ, Dims: 4, M: 2, NBits: 4,
		Segments: []*meta.VectorSegment{{Id: 0, Status: vectorstore.StatusGrowing}},
	}
	options := Options{Name: "ivfpq", StoreName: "ivfpq", StorageType: "disk"}
	manager := NewVecIndexManager(options)
	defer func() { manager.Close() }()
	idx, err := manager.Get("field", ref)
	require.NoError(t, err)
	ids, vectors := createTestVecs(4, 5100)
	_, err = idx.Batch(ctx, ref, vectors, ids, nil)
	require.NoError(t, err)
	require.Len(t, ref.Segments, 2)
	require.Equal(t, 1, ref.CurrentSegmentId)
	require.EqualValues(t, 5100, ref.Count)
	require.NoError(t, idx.Seal(ctx, ref))
	require.Equal(t, vectorstore.StatusSealed, ref.Segments[0].Status)
	ids, vectors = createTestVecs(4, 100)
	for i := range ids {
		ids[i] += 5100
	}
	_, err = idx.Batch(ctx, ref, vectors, ids, nil)
	require.NoError(t, err)
	_, err = idx.Batch(ctx, ref, nil, nil, []int64{1, 2, 3, 5150, 5151})
	require.NoError(t, err)
	require.EqualValues(t, 5097, ref.Segments[0].Count)
	require.EqualValues(t, 98, ref.Segments[1].Count)
	require.EqualValues(t, 5195, ref.Count)
	recall, err := idx.Recall(ctx, ref, 100, 10, 10)
	require.NoError(t, err)
	require.Greater(t, recall, float32(0))
	require.LessOrEqual(t, recall, float32(1))
	manager.Close()
	manager = NewVecIndexManager(options)
	idx, err = manager.Get("field", ref)
	require.NoError(t, err)
	result, err := idx.Search(ctx, ref, []float32{0.9, 0.2, 0.5, 0.6}, 10, 10)
	require.NoError(t, err)
	require.Len(t, result, 10)
	for id := range result {
		n := base62.Decode(id)
		require.True(t, 1 <= n && n <= 5200)
		require.NotContains(t, []int64{1, 2, 3, 5150, 5151}, n)
	}
}

func createTestVecs(d, count int) ([]int64, [][]float32) {
	vectors := make([][]float32, count)
	ids := make([]int64, count)
	for i := range vectors {
		vectors[i] = make([]float32, d)
		for j := range vectors[i] {
			vectors[i][j] = rand.Float32()
		}
		ids[i] = int64(i + 1)
	}
	return ids, vectors
}
