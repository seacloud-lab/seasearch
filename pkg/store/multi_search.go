package store

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/uquery/timerange"
)

type PartialIndexes map[string][]int

func (p PartialIndexes) AddSecondShardId(index string, id int) {
	p[index] = append(p[index], id)
}

func CreatePartialIndexes(index string, ids []int) PartialIndexes {
	return PartialIndexes{index: ids}
}

type IndexQueryRequest struct {
	Index         string          `json:"index"`
	SecondShardId []int           `json:"second_shard_ids"`
	Query         *meta.ZincQuery `json:"query"`
}

// MultiSearch matches owner index names and merges their results using the first
// matched index's configuration, as in the legacy query path. query is exclusive
// to this call and may be modified. All acquired references are released here.
func MultiSearch(indexNames []string, query *meta.ZincQuery) (*meta.SearchResponse, error) {
	ctx, cancel, err := queryContext(query)
	if err != nil {
		return nil, err
	}
	defer cancel()
	names, err := matchedIndexNames(indexNames)
	if err != nil {
		return nil, err
	}
	indexes, err := openQueryIndexes(names, false, false)
	if err != nil {
		return nil, err
	}
	defer closeQueryIndexes(indexes)
	if len(indexes) == 0 {
		return nil, fmt.Errorf("store.MultiSearch: error accessing reader: no index found")
	}
	unlock := lockQueryIndexes(indexes)
	defer unlock()
	err = prepareQuery(indexes[0], query)
	if err != nil {
		return nil, err
	}
	timeMin, timeMax := timerange.Query(query.Query)
	var searchers []*text.SimpleSearcher
	for _, index := range indexes {
		searchers = append(searchers, index.getSearchers(timeMin, timeMax)...)
	}
	return executeSearch(ctx, indexes[0], query, nil, searchers)
}

func matchedIndexNames(patterns []string) ([]string, error) {
	names, err := metadata.Index.ListNames(0, 0)
	if err != nil {
		return nil, err
	}
	var matched []string
	for _, name := range names {
		if !cluster.AssignCheck(name) {
			continue
		}
		match := len(patterns) == 0
		for _, pattern := range patterns {
			if matchIndexName(name, pattern) {
				match = true
				break
			}
		}
		if match {
			matched = append(matched, name)
		}
	}
	return matched, nil
}

func matchIndexName(name, pattern string) bool {
	if pattern == "" {
		return true
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(name, pattern[1:])
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(name, pattern[:len(pattern)-1])
	}
	return name == pattern
}

// Acquire every reference before taking any metadata lock. Deduplication also
// prevents recursive RLock when the same shared Index is requested twice.
func openQueryIndexes(names []string, queryViews, skipMissing bool) ([]*Index, error) {
	seen := make(map[string]bool, len(names))
	var indexes []*Index
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		var index *Index
		var err error
		if queryViews {
			index, err = GetQueryIndex(name)
		} else {
			index, err = Open(name)
		}
		if skipMissing && errors.Is(err, ErrIndexNotFound) {
			continue
		}
		if err != nil {
			closeQueryIndexes(indexes)
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func closeQueryIndexes(indexes []*Index) {
	for _, index := range indexes {
		index.Close()
	}
}

func lockQueryIndexes(indexes []*Index) func() {
	ordered := slices.Clone(indexes)
	slices.SortFunc(ordered, func(a, b *Index) int {
		return cmp.Compare(a.name, b.name)
	})
	for _, index := range ordered {
		index.mutex.RLock()
	}
	return func() {
		for i := len(ordered) - 1; i >= 0; i-- {
			ordered[i].mutex.RUnlock()
		}
	}
}
