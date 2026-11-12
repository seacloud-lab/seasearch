package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/zincsearch/zincsearch/pkg/config"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	storevector "github.com/zincsearch/zincsearch/pkg/store/vector"
	"github.com/zincsearch/zincsearch/pkg/uquery/fields"
	"github.com/zincsearch/zincsearch/pkg/uquery/source"
)

// VectorQuery preserves the vector API's wire format without depending on the
// legacy core runtime. The query and its nested values are request-exclusive.
type VectorQuery struct {
	ReturnFields interface{} `json:"return_fields"`
	QueryField   string      `json:"query_field"`
	Vector       []float32   `json:"vector"`
	K            int         `json:"k"`
	Nprobe       int         `json:"nprobe"` // Used only by IVFPQ.
	Source       interface{} `json:"_source"`
	FilterQuery  interface{} `json:"filter_query"`
}

type vectorCandidate struct {
	indexName string
	docID     string
	distance  float32
}

// VectorSearch borrows the caller's owner Index reference. The read lock covers
// filtering, native work, document retrieval and Reader cleanup. Cancellation
// never releases protection before all internal work has actually returned.
func (index *Index) VectorSearch(ctx context.Context, query *VectorQuery) (*meta.SearchResponse, error) {
	if err := validateVectorQuery(ctx, query); err != nil {
		return nil, err
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return vectorSearchLocked(ctx, []*Index{index}, query)
}

// MultiVectorSearch matches owner names using the same rules as MultiSearch.
// It owns all acquired references, takes deduplicated metadata locks in name
// order, and releases them only after every Reader and native operation ends.
// Callers must not hold a same-name Index reference while invoking this entry.
func MultiVectorSearch(ctx context.Context, indexNames []string, query *VectorQuery) (*meta.SearchResponse, error) {
	if err := validateVectorQuery(ctx, query); err != nil {
		return nil, err
	}
	names, err := matchedIndexNames(indexNames)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	indexes, err := openQueryIndexes(names, false, false)
	if err != nil {
		return nil, err
	}
	defer closeQueryIndexes(indexes)
	if len(indexes) == 0 {
		return nil, ErrIndexNotFound
	}
	unlock := lockQueryIndexes(indexes)
	defer unlock()
	return vectorSearchLocked(ctx, indexes, query)
}

func validateVectorQuery(ctx context.Context, query *VectorQuery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if query == nil {
		return fmt.Errorf("vector query must not be nil")
	}
	if query.K <= 0 || query.Nprobe <= 0 {
		return fmt.Errorf("vector query k and nprobe must be positive")
	}
	return nil
}

// vectorMetadataLocked reads the authoritative state, never an embedded core
// Index. Ownership was checked by Open; do not recheck it during a held reference.
func (index *Index) vectorMetadataLocked(field string) (*meta.VecIndex, error) {
	if index.readOnly {
		return nil, zincerrors.ErrIndexServerMismatch
	}
	if index.ref.Mappings == nil {
		return nil, fmt.Errorf("index %s has no mappings", index.name)
	}
	prop, ok := index.ref.Mappings.GetProperty(field)
	if !ok || prop.Type != "vector" {
		return nil, fmt.Errorf("field %s is not a vector field on index %s", field, index.name)
	}
	ref := index.ref.VecIndexes[field]
	if ref == nil {
		return nil, fmt.Errorf("vector field %s has no metadata on index %s", field, index.name)
	}
	if ref.Dims != prop.Dims {
		return nil, fmt.Errorf("vector field %s dimensions disagree with mapping on index %s", field, index.name)
	}
	return ref, nil
}

// All participating references and metadata read locks must already be held.
// No query path repairs layout, marks dirty or saves metadata.
func vectorSearchLocked(ctx context.Context, indexes []*Index, query *VectorQuery) (*meta.SearchResponse, error) {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Parse projection even for empty results. These request-owned values can be
	// shared read-only by the independent per-index retrieval queries.
	projection, err := source.Request(query.Source)
	if err != nil {
		return nil, err
	}
	var returnFields []*meta.Field
	if values, ok := query.ReturnFields.([]interface{}); ok {
		returnFields, err = fields.Request(values)
		if err != nil {
			return nil, err
		}
	}
	runtimes := make([]storevector.VectorIndex, len(indexes))
	refs := make([]*meta.VecIndex, len(indexes))
	// Validate every index before executing any searches. Get only constructs
	// lightweight runtime objects and validates layout; it never opens resources.
	for i, index := range indexes {
		ref, err := index.vectorMetadataLocked(query.QueryField)
		if err != nil {
			return nil, err
		}
		if ref.Dims != len(query.Vector) {
			return nil, fmt.Errorf("vector field %s requires %d dimensions on index %s", query.QueryField, ref.Dims, index.name)
		}
		runtimes[i], err = index.vector.Get(query.QueryField, ref)
		if err != nil {
			return nil, fmt.Errorf("get vector field %s on index %s: %w", query.QueryField, index.name, err)
		}
		refs[i] = ref
	}
	var candidates []vectorCandidate
	for i, index := range indexes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items, err := index.searchVectorLocked(ctx, query, runtimes[i], refs[i])
		if err != nil {
			return nil, fmt.Errorf("search vector field %s on index %s: %w", query.QueryField, index.name, err)
		}
		for _, item := range items {
			candidates = append(candidates, vectorCandidate{index.name, item.DocID, item.Distance})
		}
	}
	slices.SortFunc(candidates, func(a, b vectorCandidate) int {
		return cmp.Or(cmp.Compare(a.distance, b.distance), cmp.Compare(a.indexName, b.indexName), cmp.Compare(a.docID, b.docID))
	})
	candidates = candidates[:min(len(candidates), query.K)]
	byIndex := make(map[string][]string)
	for _, candidate := range candidates {
		byIndex[candidate.indexName] = append(byIndex[candidate.indexName], candidate.docID)
	}
	type documentKey struct{ indexName, docID string }
	documents := make(map[documentKey]meta.Hit)
	for _, index := range indexes {
		ids := byIndex[index.name]
		if len(ids) == 0 {
			continue
		}
		hits, err := index.retrieveVectorDocumentsLocked(ctx, ids, projection, returnFields)
		if err != nil {
			return nil, fmt.Errorf("retrieve vector documents on index %s: %w", index.name, err)
		}
		for id, hit := range hits {
			documents[documentKey{index.name, id}] = hit
		}
	}
	response := &meta.SearchResponse{Hits: meta.Hits{Hits: []meta.Hit{}}}
	for _, candidate := range candidates {
		hit, ok := documents[documentKey{candidate.indexName, candidate.docID}]
		if !ok {
			continue // A vector remnant is not a document; do not backfill beyond K.
		}
		hit.Score = float64(1 / (1 + candidate.distance))
		response.Hits.Hits = append(response.Hits.Hits, hit)
		response.Hits.MaxScore = max(response.Hits.MaxScore, hit.Score)
	}
	// Preserve distance tie order when float32 scoring rounds distinct distances
	// to the same score. Index/name order already breaks exact distance ties.
	slices.SortStableFunc(response.Hits.Hits, func(a, b meta.Hit) int { return cmp.Compare(b.Score, a.Score) })
	response.Hits.Total.Value = len(response.Hits.Hits)
	response.Took = int(time.Since(started).Milliseconds())
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return response, nil
}

func (index *Index) searchVectorLocked(ctx context.Context, query *VectorQuery, runtime storevector.VectorIndex, ref *meta.VecIndex) ([]storevector.DocDistance, error) {
	if query.FilterQuery == nil {
		result, err := runtime.Search(ctx, ref, query.Vector, int64(query.K), query.Nprobe)
		if err != nil {
			return nil, err
		}
		items := make([]storevector.DocDistance, 0, len(result))
		for id, distance := range result {
			items = append(items, storevector.DocDistance{DocID: id, Distance: distance})
		}
		return items, nil
	}
	filter := &meta.ZincQuery{Query: query.FilterQuery, Sort: []any{"_id"}, Size: config.Global.MaxResults}
	response, err := index.partialSearchLocked(ctx, nil, filter, nil)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var ids []string
	for _, hit := range response.Hits.Hits {
		if !seen[hit.ID] {
			seen[hit.ID] = true
			ids = append(ids, hit.ID)
		}
	}
	if len(ids) == 0 {
		return nil, ctx.Err()
	}
	return runtime.SearchByIDs(ctx, ref, query.Vector, int64(query.K), ids)
}

// Retrieve IDs only from their owning index, with that index's mappings. Bound
// each window by MaxResults. Remove retrieved IDs before querying again so old
// text copies cannot hide another selected ID or inflate the final total.
func (index *Index) retrieveVectorDocumentsLocked(ctx context.Context, ids []string, projection *meta.Source, returnFields []*meta.Field) (map[string]meta.Hit, error) {
	hits := make(map[string]meta.Hit)
	limit := config.Global.MaxResults
	if limit <= 0 {
		return nil, fmt.Errorf("max results must be positive")
	}
	for start := 0; start < len(ids); {
		end := start + min(limit, len(ids)-start)
		window := ids[start:end]
		wanted := make(map[string]bool, len(window))
		for _, id := range window {
			wanted[id] = true
		}
		for len(wanted) > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			remaining := make([]string, 0, len(wanted))
			for _, id := range window {
				if wanted[id] {
					remaining = append(remaining, id)
				}
			}
			query := &meta.ZincQuery{
				Query: &meta.Query{Ids: &meta.IdsQuery{Values: remaining}},
				Size:  len(remaining), Sort: []any{"_id"},
				Source: projection, Fields: returnFields,
			}
			response, err := index.partialSearchLocked(ctx, nil, query, nil)
			if err != nil {
				return nil, err
			}
			before := len(wanted)
			for _, hit := range response.Hits.Hits {
				if wanted[hit.ID] {
					hits[hit.ID] = hit
					delete(wanted, hit.ID)
				}
			}
			if len(wanted) == before {
				break
			}
		}
		start = end
	}
	return hits, nil
}
