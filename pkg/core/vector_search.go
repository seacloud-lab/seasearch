package core

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/blugelabs/bluge"
	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/fields"
	"github.com/zincsearch/zincsearch/pkg/uquery/source"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
	"golang.org/x/sync/errgroup"
)

type VectorQuery struct {
	ReturnFields interface{} `json:"return_fields"`
	QueryField   string      `json:"query_field"`
	Vector       []float32   `json:"vector"`
	K            int64       `json:"k"`
	// Nprobe only used for ivf_pq index
	Nprobe int         `json:"nprobe"`
	Source interface{} `json:"_source"`
}

func VectorSearch(zincIndex *Index, mappings *meta.Mappings, q *VectorQuery) (*meta.SearchResponse, error) {
	vecIndex, err := GetVectorIndex(zincIndex.GetName(), q.QueryField)
	if err != nil {
		if errors.Is(err, ErrVecIndexNotExists) {
			return &meta.SearchResponse{Hits: meta.Hits{Hits: []meta.Hit{}}}, nil
		}
		return nil, err
	}
	defer func() {
		CloseVectorIndex(vecIndex, false)
	}()

	if vecIndex.Meta().Dims != len(q.Vector) {
		return nil, fmt.Errorf("invalid query vector, the vector dims should be %d", vecIndex.Meta().Dims)
	}
	vecIndex.RLock()
	vecResult, err := vecIndex.Search(q.Vector, q.K, q.Nprobe)
	vecIndex.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(vecResult) == 0 {
		return &meta.SearchResponse{Hits: meta.Hits{Hits: []meta.Hit{}}}, nil
	}

	docIdSlice := make([]string, len(vecResult))
	i := 0
	for docId := range vecResult {
		docIdSlice[i] = docId
		i++
	}

	readers, err := zincIndex.GetReaders(0, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()

	// query document by Id
	dmi, err := zincsearch.MultiSearch(context.Background(), &meta.ZincQuery{
		Query: &meta.Query{
			Ids: &meta.IdsQuery{
				Values: docIdSlice,
			},
		},
		Size: len(docIdSlice),
	}, mappings, nil, ToSearcher(readers)...)
	if err != nil {
		return nil, err
	}

	query := &meta.ZincQuery{}
	query.Source, err = source.Request(q.Source)
	if err != nil {
		return nil, err
	}
	if q.ReturnFields != nil {
		if v, ok := q.ReturnFields.([]interface{}); ok {
			if query.Fields, err = fields.Request(v); err != nil {
				return nil, err
			}
		}
	}
	resp, err := searchV2(zincIndex.GetAllShardNum(), int64(len(readers)), dmi, query, mappings)
	if err != nil {
		return nil, err
	}
	// set score by distance
	var maxScore float64 = 0
	for i := range resp.Hits.Hits {
		h1d, ok := vecResult[resp.Hits.Hits[i].ID]
		if !ok {
			// that's impassable
			resp.Hits.Hits[i].Score = 0
			continue
		}
		score := float64(1 / (1 + h1d))
		resp.Hits.Hits[i].Score = score
		maxScore = math.Max(maxScore, score)
	}
	// sort by score
	sort.Slice(resp.Hits.Hits, func(i, j int) bool {
		return resp.Hits.Hits[i].Score > resp.Hits.Hits[j].Score
	})
	resp.Hits.MaxScore = maxScore
	return resp, nil
}

func VectorRecall(zincIndex *Index, field string, querySize int, k int64, nprobe int) (float32, error) {
	vecIndexMeta, ok := zincIndex.GetVecIndex(field)
	if !ok {
		return 0, ErrVecIndexNotExists
	}

	if vecIndexMeta.Type == vector.Flat {
		return 1.0, nil
	}

	idx, err := GetVectorIndex(zincIndex.GetName(), field)
	if err != nil {
		return 0, err
	}
	defer func() {
		CloseVectorIndex(idx, false)
	}()
	vecIndex, ok := idx.(*IvfPqIndex)
	if !ok {
		return 0, fmt.Errorf("invlaid vec index type: %T", idx)
	}
	xq, err := getQueryVectors(vecIndex, querySize)
	if err != nil {
		return 0, err
	}

	// build a temp flat index
	flatIdx, err := createTempFlatIndex(vecIndex)
	if err != nil {
		return 0, err
	}
	defer func() {
		// free memory
		flatIdx.Delete()
	}()

	correct := int64(0)

	for _, query := range xq {
		vecIndex.RLock()
		results, err := vecIndex.Search(query, k, nprobe)
		vecIndex.RUnlock()
		if err != nil {
			return 0, err
		}
		_, ids, err := flatIdx.Search(query, k)
		if err != nil {
			return 0, err
		}
		for _, id := range ids {
			if _, ok := results[base62.Encode(id)]; ok {
				correct++
			}
		}
	}

	recall := float32(correct) / float32(k*int64(querySize))

	return recall, nil
}

// createTempFlatIndex
// get vectors from bluge and build a temp flat index.
func createTempFlatIndex(vecIndex *IvfPqIndex) (faiss.Index, error) {
	// get documents
	readers, err := vecIndex.zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	q := bluge.NewMatchAllQuery()
	req := bluge.NewAllMatches(q)
	vectors, ids, err := getVectors(vecIndex.field, vecIndex.ref.Dims, vecIndex.ref.Count, req, readers...)
	if err != nil {
		return nil, err
	}

	var idx faiss.Index

	idx, err = faiss.IndexFactory(vecIndex.ref.Dims, "IDMap,Flat", faiss.MetricL2)
	if err != nil {
		return nil, err
	}
	err = idx.AddWithIDs(vectors, ids)
	if err != nil {
		idx.Delete()
		return nil, err
	}

	return idx, nil
}

// getQueryVectors
// get some vectors from bluge
func getQueryVectors(vecIndex *IvfPqIndex, n int) ([][]float32, error) {
	dim := vecIndex.ref.Dims
	// get documents
	readers, err := vecIndex.zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return nil, err
	}
	q := bluge.NewMatchAllQuery()
	req := bluge.NewTopNSearch(n, q)
	vectors, _, err := getVectors(vecIndex.field, dim, int64(n), req, readers...)
	if err != nil {
		return nil, err
	}
	res := make([][]float32, 0, n)
	for i := 0; i < n; i++ {
		res = append(res, vectors[i*dim:(i+1)*dim])
	}
	return res, nil
}

type vecDoc struct {
	docId string
	vec   []float32
}

// getVectors get vector from bluge
func getVectors(field string, d int, count int64, searchReq bluge.SearchRequest, readers ...*bluge.Reader) ([]float32, []int64, error) {
	ch := make(chan vecDoc, len(readers)*10)
	eg := &errgroup.Group{}
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	for _, reader := range readers {
		r := reader
		eg.Go(func() error {
			dmi, err := r.Search(context.Background(), searchReq)
			if err != nil {
				return err
			}
			for next, err := dmi.Next(); err == nil && next != nil; next, err = dmi.Next() {
				var id string
				var vec []float32
				err = next.VisitStoredFields(func(f string, value []byte) bool {
					if f == "_id" {
						id = string(value)
						return vec == nil
					}
					if f == field {
						vec = zutils.BytesToVector(value)
						return id == ""
					}
					return true
				})
				ch <- vecDoc{
					vec:   vec,
					docId: id,
				}
				if err != nil {
					return err
				}
			}
			return err
		})
	}

	var vectors []float32
	var ids []int64
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		vectors = make([]float32, 0, int(count)*d)
		ids = make([]int64, 0, count)
		for doc := range ch {
			vectors = append(vectors, doc.vec...)
			ids = append(ids, base62.Decode(doc.docId))
		}
	}()

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	close(ch)
	wg.Wait()

	return vectors, ids, nil
}
