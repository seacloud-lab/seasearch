package core

import (
	"context"
	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"math"
	"sort"
)

type VectorQuery struct {
	ReturnFields []string  `json:"return_fields"`
	QueryField   string    `json:"query_field"`
	Vector       []float32 `json:"vector"`
	K            int64     `json:"k"`
	// Nprobe only used for ivf_pq index
	Nprobe int `json:"nprobe"`
}

func VectorSearch(zincIndex *Index, mappings *meta.Mappings, q *VectorQuery) (*meta.SearchResponse, error) {
	vecIndex, err := GetVectorIndex(zincIndex.GetName(), q.QueryField)
	if err != nil {
		if errors.Is(err, ErrVecIndexNotExists) {
			return &meta.SearchResponse{Hits: meta.Hits{Hits: []meta.Hit{}}}, nil
		}
		return nil, err
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
	for docId, _ := range vecResult {
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
	}, mappings, nil, readers...)
	if err != nil {
		return nil, err
	}

	fields := make([]*meta.Field, 0)
	query := &meta.ZincQuery{
		Fields: []*meta.Field{},
		Source: &meta.Source{
			Enable: true,
		},
	}
	for _, f := range q.ReturnFields {
		fields = append(fields, &meta.Field{
			Field: f,
		})
	}
	query.Fields = fields
	resp, err := searchV2(zincIndex.GetAllShardNum(), int64(len(readers)), dmi, query, mappings)
	if err != nil {
		return nil, err
	}
	// set score by distance
	var maxScore float64 = 0
	for i, _ := range resp.Hits.Hits {
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
