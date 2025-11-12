package core

import (
	"cmp"
	"context"
	"fmt"
	"slices"

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
	K            int         `json:"k"`
	// Nprobe only used for ivf_pq index
	Nprobe int         `json:"nprobe"`
	Source interface{} `json:"_source"`
}

type vectorResult struct {
	docID    string
	distance float32
}

func VectorSearch(indexes []*Index, q *VectorQuery) (*meta.SearchResponse, error) {
	var results []vectorResult
	for _, index := range indexes {
		result, err := searchVector(index, q)
		if err != nil {
			return nil, err
		}
		results = append(results, result...)
	}
	if len(results) == 0 {
		return &meta.SearchResponse{Hits: meta.Hits{Hits: []meta.Hit{}}}, nil
	}

	slices.SortFunc(results, func(a, b vectorResult) int {
		return cmp.Compare(a.distance, b.distance)
	})
	if len(results) > q.K {
		results = results[:q.K]
	}

	return retrieveVectorDocuments(indexes, q, results)
}

func searchVector(index *Index, q *VectorQuery) ([]vectorResult, error) {
	vecIndex, err := GetVectorIndex(index.GetName(), q.QueryField, false)
	if errors.Is(err, ErrVecIndexNotExists) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer CloseVectorIndex(vecIndex, false)

	if vecIndex.Meta().Dims != len(q.Vector) {
		return nil, fmt.Errorf("invalid query vector, the vector dims should be %d", vecIndex.Meta().Dims)
	}
	result, err := vecIndex.Search(q.Vector, int64(q.K), q.Nprobe)
	if err != nil {
		return nil, err
	}
	var results []vectorResult
	for k, v := range result {
		results = append(results, vectorResult{docID: k, distance: v})
	}

	return results, nil
}

func retrieveVectorDocuments(indexes []*Index, q *VectorQuery, results []vectorResult) (*meta.SearchResponse, error) {
	var (
		docIDs    []string
		err       error
		searchers []*SimpleSearcher
	)
	for _, result := range results {
		docIDs = append(docIDs, result.docID)
	}
	query := &meta.ZincQuery{
		Query: &meta.Query{
			Ids: &meta.IdsQuery{
				Values: docIDs,
			},
		},
		Size: len(docIDs),
	}
	query.Source, err = source.Request(q.Source)
	if err != nil {
		return nil, err
	}
	if q.ReturnFields != nil {
		if v, ok := q.ReturnFields.([]any); ok {
			if query.Fields, err = fields.Request(v); err != nil {
				return nil, err
			}
		}
	}
	for _, index := range indexes {
		searchers = append(searchers, index.GetSearchers(0, 0)...)
	}
	resp, err := zincsearch.MultiSearch(context.Background(), query, indexes[0].GetMappings(), nil, simpleSearchersToSearcher(searchers)...)
	if err != nil {
		return nil, err
	}

	for i, hit := range resp.Hits.Hits {
		j := slices.IndexFunc(results, func(result vectorResult) bool {
			return result.docID == hit.ID
		})
		if j < 0 {
			resp.Hits.Hits[i].Score = float64(0)
			continue
		}
		score := float64(1 / (1 + results[j].distance))
		resp.Hits.Hits[i].Score = score
		if score > resp.Hits.MaxScore {
			resp.Hits.MaxScore = score
		}
	}
	slices.SortFunc(resp.Hits.Hits, func(a, b meta.Hit) int {
		return -cmp.Compare(a.Score, b.Score)
	})

	return resp, nil
}

type InternalVectorQuery struct {
	Index      string    `json:"index"`
	QueryField string    `json:"query_field"`
	K          int       `json:"k"`
	Vector     []float32 `json:"vector"`
	Nprobe     int       `json:"nprobe"`
	Segments   []int     `json:"segments"`
}

type InternalVectorSearchResponse struct {
	MatchedIds map[string]float32 `json:"matched_ids"`
}

// InternalVectorSearch queries the specified vector index segments.
func InternalVectorSearch(q *InternalVectorQuery) (*InternalVectorSearchResponse, error) {
	vecIndex, err := GetVectorIndex(q.Index, q.QueryField, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		CloseVectorIndex(vecIndex, false)
	}()
	if vecIndex.Meta().Dims != len(q.Vector) {
		return nil, fmt.Errorf("invalid query vector, the vector dims should be %d", vecIndex.Meta().Dims)
	}
	// If parallel search is not required,
	// the request should be forwarded directly to the corresponding node.
	if len(q.Segments) == 0 {
		return nil, fmt.Errorf("invalid segment ids")
	}

	var vecResult map[string]float32
	vecResult, err = vecIndex.PartialSearch(q.Vector, int64(q.K), q.Nprobe, q.Segments)
	if err != nil {
		return nil, err
	}

	return &InternalVectorSearchResponse{MatchedIds: vecResult}, nil
}

func VectorRecall(zincIndex *Index, field string, querySize int, k int64, nprobe int) (float32, error) {
	vecIndexMeta, ok := zincIndex.GetVecIndex(field)
	if !ok {
		return 0, ErrVecIndexNotExists
	}

	if vecIndexMeta.TargetType == vector.Flat {
		return 1.0, nil
	}

	idx, err := GetVectorIndex(zincIndex.GetName(), field, false)
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
