package store

import (
	"context"
	"fmt"
	"time"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/uquery"
	"github.com/zincsearch/zincsearch/pkg/uquery/timerange"
)

// GetSearchers selects shards using store-owned metadata without opening Readers
// or Writers. It does not acquire an additional Index reference. The caller must
// hold its Index reference through result consumption and close every returned
// Searcher before releasing it. Readers borrowed from Searchers must not be
// closed separately.
func (index *Index) GetSearchers(timeMin, timeMax int64) []*text.SimpleSearcher {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	return index.getSearchers(timeMin, timeMax)
}

// GetSearchersByID selects only the specified second shards and follows the same
// reference and cleanup contract as GetSearchers. Shard IDs are assigned by the
// proxy for collaborative queries; an empty ID list selects no shards.
func (index *Index) GetSearchersByID(timeMin, timeMax int64, secondShardIDs ...int) []*text.SimpleSearcher {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	return index.getSearchersByID(timeMin, timeMax, secondShardIDs...)
}

// getSearchers requires index.mutex's read or write lock and does not acquire it.
func (index *Index) getSearchers(timeMin, timeMax int64) []*text.SimpleSearcher {
	return index.text.GetSearchers(index.ref.Shards, timeMin, timeMax)
}

// getSearchersByID requires index.mutex's read or write lock and does not acquire it.
func (index *Index) getSearchersByID(timeMin, timeMax int64, ids ...int) []*text.SimpleSearcher {
	return index.text.GetSearchersByID(index.ref.Shards, timeMin, timeMax, ids...)
}

// Search consumes the result and closes all Readers before returning. The caller
// must hold an Index reference. Queries are mutable, request-exclusive objects.
func (index *Index) Search(query *meta.ZincQuery) (*meta.SearchResponse, error) {
	return index.SearchWithStats(query, nil)
}

func (index *Index) SearchWithStats(query *meta.ZincQuery, stats *zincsearch.UnifiedStats) (*meta.SearchResponse, error) {
	return index.PartialSearch(nil, query, stats)
}

// PartialSearch treats an empty ID list as all shards. stats is borrowed read-only;
// nil uses ordinary scoring. It does not acquire or release an Index reference.
func (index *Index) PartialSearch(ids []int, query *meta.ZincQuery, stats *zincsearch.UnifiedStats) (*meta.SearchResponse, error) {
	ctx, cancel, err := queryContext(query)
	if err != nil {
		return nil, err
	}
	defer cancel()

	index.mutex.RLock()
	defer index.mutex.RUnlock()

	return index.partialSearch(ctx, ids, query, stats)
}

// partialSearch requires index.mutex's read or write lock and an Index reference.
// It consumes results and closes every Searcher/Reader before returning, including
// on failure. Callers must not use a partial response when an error is returned.
func (index *Index) partialSearch(ctx context.Context, ids []int, query *meta.ZincQuery, stats *zincsearch.UnifiedStats) (*meta.SearchResponse, error) {
	err := prepareQuery(index, query)
	if err != nil {
		return nil, err
	}
	timeMin, timeMax := timerange.Query(query.Query)
	var searchers []*text.SimpleSearcher
	if len(ids) == 0 {
		searchers = index.getSearchers(timeMin, timeMax)
	} else {
		searchers = index.getSearchersByID(timeMin, timeMax, ids...)
	}
	return executeSearch(ctx, index, query, stats, searchers)
}

func queryContext(query *meta.ZincQuery) (context.Context, context.CancelFunc, error) {
	if query == nil {
		return nil, nil, fmt.Errorf("query must not be nil")
	}
	if query.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(query.Timeout)*time.Second)
		return ctx, cancel, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, nil
}

// prepareQuery requires the Index metadata lock to be held.
func prepareQuery(index *Index, query *meta.ZincQuery) error {
	err := uquery.NormalizeQuery(query, index.ref.Mappings, index.analyzers)
	if err != nil {
		return err
	}
	_, err = uquery.ParseQueryDSL(query, index.ref.Mappings, index.analyzers)
	return err
}

// All participating Index metadata locks remain held through execution and cleanup.
func executeSearch(ctx context.Context, index *Index, query *meta.ZincQuery, stats *zincsearch.UnifiedStats, bases []*text.SimpleSearcher) (*meta.SearchResponse, error) {
	searchers := make([]zincsearch.Searcher, len(bases))
	for i, base := range bases {
		if stats == nil {
			searchers[i] = base
		} else {
			searchers[i] = text.NewUnifiedSearcher(base, stats)
		}
	}
	defer func() {
		for _, searcher := range searchers {
			searcher.Close()
		}
	}()
	err := ctx.Err()
	if err != nil {
		return nil, err
	}
	return zincsearch.MultiZincSearch(ctx, query, index.ref.Mappings, index.analyzers, searchers...)
}
