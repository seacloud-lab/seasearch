/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/uquery"
	query2 "github.com/zincsearch/zincsearch/pkg/uquery/query"
	"github.com/zincsearch/zincsearch/pkg/uquery/timerange"
	"golang.org/x/sync/errgroup"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

func MultiSearch(indexNames []string, query *meta.ZincQuery) (*meta.SearchResponse, error) {
	matchedNames, err := GetMatchedIndexNames(indexNames)
	if err != nil {
		return nil, err
	}
	return MultiSearchWithStats(matchedNames, nil, query)
}

// MultiSearchWithStats searches multiple indexes with the input query.
// If stats is not nil, it will use unified stats for score calculation;
// otherwise each index will use its own stats.
func MultiSearchWithStats(searchIndexNames []string, stats *zincsearch.UnifiedStats, query *meta.ZincQuery) (*meta.SearchResponse, error) {
	var mappings *meta.Mappings
	var analyzers map[string]*analysis.Analyzer
	searchers := make([]*SimpleSearcher, 0)
	var shardNum int64

	hasIndex := false
	searchIndex := make([]*Index, 0)
	timeMin, timeMax := timerange.Query(query.Query)

	for _, indexName := range searchIndexNames {
		// this index should not handle by this servers
		if !cluster.AssignCheck(indexName) {
			return nil, ErrIndexServerMismatch
		}
		index, ok := ZINC_INDEX_LIST.Get(indexName)
		if ok {
			hasIndex = true
			searchIndex = append(searchIndex, index)
		}
	}

	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.LoadObjGoroutineNum)

	for _, index := range searchIndex {
		searchers = append(searchers, index.GetSearchers(timeMin, timeMax)...)
		shardNum += int64(len(searchers))
		if mappings == nil {
			mappings = index.GetMappings()
			analyzers = index.GetAnalyzers()
		}
	}

	if len(searchers) == 0 {
		if !hasIndex {
			return nil, fmt.Errorf("core.MultiSearchV2: error accessing reader: no index found")
		}
		return &meta.SearchResponse{}, nil
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if query.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(query.Timeout)*time.Second)
		defer cancel()
	}

	if stats != nil {
		return zincsearch.MultiSearch(ctx, query, mappings, analyzers, simpleSearchersToUnifiedSearcher(stats, searchers)...)
	}

	return zincsearch.MultiSearch(ctx, query, mappings, analyzers, simpleSearchersToSearcher(searchers)...)
}

// QueryStatsInfo get statistics from specified indexes, returns statistics for all index mergers.
func QueryStatsInfo(indexNames []string, query *meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	hasIndex := false
	var mappings *meta.Mappings
	var analyzers map[string]*analysis.Analyzer
	var searchers = make([]*SimpleSearcher, 0)

	timeMin, timeMax := timerange.Query(query.Query)

	for _, indexName := range indexNames {
		// this index should not handle by this servers
		if !cluster.AssignCheck(indexName) {
			return nil, ErrIndexServerMismatch
		}
		index, ok := ZINC_INDEX_LIST.Get(indexName)
		if ok {
			hasIndex = true
			if mappings == nil {
				mappings = index.GetMappings()
				analyzers = index.GetAnalyzers()
			}
			searchers = append(searchers, index.GetSearchers(timeMin, timeMax)...)
		}
	}

	if len(searchers) == 0 {
		if !hasIndex {
			return nil, fmt.Errorf("core.UnifySearch: error accessing reader: no index found")
		}
		return nil, nil
	}

	termList, err := query2.QueryTerms(query.Query, mappings, analyzers)
	if err != nil {
		return nil, err
	}

	return getLocalStatsInfo(searchers, termList)
}

func simpleSearchersToSearcher(readers []*SimpleSearcher) []zincsearch.Searcher {
	searchReaders := make([]zincsearch.Searcher, len(readers))
	for i, r := range readers {
		searchReaders[i] = r
	}
	return searchReaders
}

func simpleSearchersToUnifiedSearcher(stats *zincsearch.UnifiedStats, searchers []*SimpleSearcher) []zincsearch.Searcher {
	result := make([]zincsearch.Searcher, len(searchers))
	for i, r := range searchers {
		result[i] = &UnifiedSearcher{
			stats:         stats,
			secondShardID: r.secondShardID,
			shard:         r.shard,
		}
	}
	return result
}

// isMatchIndex("abc", "a")  false
// isMatchIndex("abc", "a*") true
// isMatchIndex("abc", "*bc") true
// isMatchIndex("abc", "bc") false
// isMatchIndex("abc", "abc") true
func isMatchIndex(zincIndexName, indexName string) bool {
	if indexName == "" {
		return true
	}

	// eg.: *-test
	if strings.HasPrefix(indexName, "*") {
		return strings.HasSuffix(zincIndexName, indexName[1:])
	}

	// eg.: test-*
	if strings.HasSuffix(indexName, "*") {
		return strings.HasPrefix(zincIndexName, indexName[:len(indexName)-1])
	}

	return zincIndexName == indexName
}

// GetMatchedIndexNames
// return all matched index names,if input indexNames is empty, we return all indexes
func GetMatchedIndexNames(indexNames []string) ([]string, error) {
	if len(indexNames) == 0 {
		return ZINC_INDEX_LIST.ListName(), nil
	}
	var needMatchIndexes []string
	var indexes []string
	for _, name := range indexNames {
		if name == "" || strings.HasSuffix(name, "*") || strings.HasPrefix(name, "*") {
			needMatchIndexes = append(needMatchIndexes, name)
		} else {
			indexes = append(indexes, name)
		}
	}
	searchIndex := make([]string, 0)
	if len(needMatchIndexes) > 0 {
		for _, index := range ZINC_INDEX_LIST.List() {
			for _, indexName := range needMatchIndexes {
				isMatched := isMatchIndex(index.GetName(), indexName)
				if isMatched {
					searchIndex = append(searchIndex, index.GetName())
					break
				}
			}
		}
	}

	for _, index := range indexes {
		_, ok := ZINC_INDEX_LIST.Get(index)
		if ok {
			searchIndex = append(searchIndex, index)
		}
	}

	return searchIndex, nil
}

type PartialIndexes map[string][]int

func (p PartialIndexes) AddSecondShardId(index string, id int) {
	if ids, ok := p[index]; ok {
		ids = append(ids, int(id))
		p[index] = ids
	} else {
		p[index] = []int{int(id)}
	}
}

func CreatePartialIndexes(index string, ids []int) PartialIndexes {
	p := make(PartialIndexes)
	p[index] = ids
	return p
}

type IndexQueryRequest struct {
	Index         string          `json:"index"`
	SecondShardId []int           `json:"second_shard_ids"`
	Query         *meta.ZincQuery `json:"query"`
}

// PartialSearch queries the specified secondary shard with given statistics information.
// If no secondary shard IDs are provided, it searches the entire index.
func (index *Index) PartialSearch(secondShardIds []int, query *meta.ZincQuery, stats *zincsearch.UnifiedStats) (*meta.SearchResponse, error) {
	mappings := index.GetMappings()
	analyzers := index.GetAnalyzers()
	err := uquery.NormalizeQuery(query, mappings, analyzers)
	if err != nil {
		return nil, err
	}
	_, err = uquery.ParseQueryDSL(query, mappings, analyzers)
	if err != nil {
		return nil, err
	}

	timeMin, timeMax := timerange.Query(query.Query)
	var searchers []*SimpleSearcher
	if len(secondShardIds) == 0 {
		searchers = index.GetSearchers(timeMin, timeMax)
	} else {
		searchers = index.GetSearchersByID(timeMin, timeMax, secondShardIds...)
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if query.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(query.Timeout)*time.Second)
		defer cancel()
	}

	return zincsearch.MultiSearch(ctx, query, mappings, analyzers, simpleSearchersToUnifiedSearcher(stats, searchers)...)
}

// QueryStatsInfoWithSecondShardIds retrieves statistics from specified indexes and secondary shards.
// It returns statistics for all index mergers.
// If no secondary shard IDs are provided, statistics for the entire index are collected.
// This is useful in scenarios where both large-scale indexes and smaller indexes
// are being searched together in a single request.
func QueryStatsInfoWithSecondShardIds(indexes PartialIndexes, query *meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	var mappings *meta.Mappings
	var analyzers map[string]*analysis.Analyzer
	var searchers = make([]*SimpleSearcher, 0)
	timeMin, timeMax := timerange.Query(query.Query)

	for idx, secondIds := range indexes {
		index, err := GetZincIndexFromMetadata(idx)
		if err != nil {
			return nil, err
		}
		if mappings == nil {
			mappings = index.GetMappings()
			analyzers = index.GetAnalyzers()
		}
		if len(secondIds) == 0 {
			searchers = append(searchers, index.GetSearchers(timeMin, timeMax)...)
		} else {
			searchers = append(searchers, index.GetSearchersByID(timeMin, timeMax, secondIds...)...)
		}
	}

	if len(searchers) == 0 {
		return &zincsearch.UnifiedStats{}, nil
	}
	// we assume that all indexes have same mappings and analyzers.
	termList, err := query2.QueryTerms(query.Query, mappings, analyzers)
	if err != nil {
		return nil, err
	}
	return getLocalStatsInfo(searchers, termList)
}

func getLocalStatsInfo(searchers []*SimpleSearcher, termList []query2.Term) (*zincsearch.UnifiedStats, error) {
	result := &zincsearch.UnifiedStats{
		FieldStats: map[string]*zincsearch.FieldStats{},
	}

	// field -> []terms
	termMap := make(map[string][]query2.Term)
	for _, term := range termList {
		if tms, ok := termMap[term.Field]; ok {
			termMap[term.Field] = append(tms, term)
		} else {
			termMap[term.Field] = []query2.Term{term}
		}
	}

	opened := make([]zincsearch.Searcher, 0)
	defer func() {
		for _, s := range opened {
			_ = s.Close()
		}
	}()
	for _, searcher := range searchers {
		reader, err := searcher.GetReader()
		if err != nil {
			return nil, err
		}
		if reader == nil {
			continue
		}
		opened = append(opened, reader)
		for field, terms := range termMap {
			fieldStats, err := getFieldStats(reader, field, terms)
			if err != nil {
				return nil, err
			}
			result.MergeField(fieldStats)
		}
		_ = reader.Close()
	}

	return result, nil
}

func getFieldStats(reader *bluge.Reader, field string, terms []query2.Term) (*zincsearch.FieldStats, error) {
	res := &zincsearch.FieldStats{
		Field:     field,
		TermStats: map[string]*zincsearch.TermStats{},
	}
	stats, err := reader.GetUnderlyingReader().CollectionStats(field)
	if err != nil {
		return nil, err
	}
	res.Merge(stats)

	for _, term := range terms {
		freq, err := getTermFreq(reader, term.Field, term.Value)
		if err != nil {
			return nil, err
		}
		key := base64.StdEncoding.EncodeToString(term.Value)
		if termStats, ok := res.TermStats[key]; ok {
			termStats.Freq += freq
		} else {
			res.TermStats[key] = &zincsearch.TermStats{
				Value: key,
				Freq:  freq,
			}
		}
	}

	return res, nil
}

func getTermFreq(reader *bluge.Reader, field string, term []byte) (uint64, error) {
	var res uint64
	c, err := reader.GetUnderlyingReader().GetDocumentFreq(term, field, true, true, false)
	if err != nil {
		return 0, err
	}
	res += c
	return res, nil
}
