package core

import (
	"context"
	"fmt"
	"math"
	"sort"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/fields"
	"github.com/zincsearch/zincsearch/pkg/uquery/source"
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

	vecResult, err := vecIndex.Search(q.Vector, q.K, q.Nprobe)

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

	if vecIndexMeta.TargetType == vector.Flat {
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

	return vecIndex.Recall(querySize, k, nprobe)
}
