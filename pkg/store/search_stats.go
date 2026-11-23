package store

import (
	"context"
	"slices"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"
	"github.com/zincsearch/zincsearch/pkg/uquery/timerange"
)

// QueryStatsInfo collects owner statistics, skipping missing indexes. It borrows
// a request-exclusive query and owns all Index references acquired for this call.
func QueryStatsInfo(names []string, q *meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	ctx, cancel, err := queryContext(q)
	if err != nil {
		return nil, err
	}
	defer cancel()
	indexes, err := openQueryIndexes(names, false, true)
	if err != nil {
		return nil, err
	}
	defer closeQueryIndexes(indexes)
	unlock := lockQueryIndexes(indexes)
	defer unlock()
	return collectQueryStats(ctx, indexes, nil, q)
}

// QueryStatsInfoWithSecondShardIds permits non-owner read-only views. An empty
// shard list means all shards; missing indexes are returned as errors.
func QueryStatsInfoWithSecondShardIds(partial PartialIndexes, q *meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	ctx, cancel, err := queryContext(q)
	if err != nil {
		return nil, err
	}
	defer cancel()
	names := make([]string, 0, len(partial))
	for name := range partial {
		names = append(names, name)
	}
	slices.Sort(names)
	indexes, err := openQueryIndexes(names, true, false)
	if err != nil {
		return nil, err
	}
	defer closeQueryIndexes(indexes)
	unlock := lockQueryIndexes(indexes)
	defer unlock()
	return collectQueryStats(ctx, indexes, partial, q)
}

// collectQueryStats requires all participating Index metadata locks to be held.
func collectQueryStats(ctx context.Context, indexes []*Index, partial PartialIndexes, q *meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	err := ctx.Err()
	if err != nil {
		return nil, err
	}
	if len(indexes) == 0 {
		return &zincsearch.UnifiedStats{}, nil
	}
	// Preserve the existing assumption that participating indexes share mappings
	// and analyzers. Parse terms before creating any request resources.
	terms, err := query.QueryTerms(q.Query, indexes[0].ref.Mappings, indexes[0].analyzers)
	if err != nil {
		return nil, err
	}
	timeMin, timeMax := timerange.Query(q.Query)
	var searchers []*text.SimpleSearcher
	for _, index := range indexes {
		ids := partial[index.name]
		if len(ids) == 0 {
			searchers = append(searchers, index.getSearchers(timeMin, timeMax)...)
		} else {
			searchers = append(searchers, index.getSearchersByID(timeMin, timeMax, ids...)...)
		}
	}
	return text.CollectStats(ctx, searchers, terms)
}
