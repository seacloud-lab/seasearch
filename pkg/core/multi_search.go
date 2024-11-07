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
	"sync"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	query2 "github.com/zincsearch/zincsearch/pkg/uquery/query"
	"golang.org/x/sync/errgroup"

	"github.com/blugelabs/bluge/analysis"
	"github.com/rs/zerolog/log"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery"
	"github.com/zincsearch/zincsearch/pkg/uquery/timerange"
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
	var readers []*bluge.Reader
	var shardNum int64

	timeMin, timeMax := timerange.Query(query.Query)
	hasIndex := false
	searchIndex := make([]*Index, 0)

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

	mutex := sync.Mutex{}
	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.LoadObjGoroutineNum)
	for _, index := range searchIndex {
		index := index
		eg.Go(func() error {
			reader, err := index.GetReaders(timeMin, timeMax)
			if err != nil {
				return err
			}
			mutex.Lock()
			readers = append(readers, reader...)
			shardNum += index.GetShardNum()
			if mappings == nil {
				mappings = index.GetMappings()
				analyzers = index.GetAnalyzers()
			}
			mutex.Unlock()
			return nil
		})
	}

	err := eg.Wait()
	if err != nil {
		return nil, err
	}
	if len(readers) == 0 {
		if !hasIndex {
			return nil, fmt.Errorf("core.MultiSearchV2: error accessing reader: no index found")
		}
		return &meta.SearchResponse{}, nil
	}

	_, err = uquery.ParseQueryDSL(query, mappings, analyzers)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if query.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(query.Timeout)*time.Second)
		defer cancel()
	}

	var searchers []zincsearch.Searcher
	if stats != nil {
		searchers = make([]zincsearch.Searcher, len(readers))
		for i, r := range readers {
			searchers[i] = zincsearch.NewUnifiedSearcher(r, stats)
		}
	} else {
		searchers = ToSearcher(readers)
	}

	dmi, err := zincsearch.MultiSearch(ctx, query, mappings, analyzers, searchers...)
	if err != nil {
		log.Printf("core.MultiSearchV2: error executing search: %s", err.Error())
		if err == context.DeadlineExceeded {
			return &meta.SearchResponse{
				TimedOut: true,
				Error:    err.Error(),
				Hits:     meta.Hits{Hits: []meta.Hit{}},
			}, nil
		}
		return nil, err
	}

	return searchV2(shardNum, int64(len(readers)), dmi, query, mappings)
}

func ToSearcher(readers []*bluge.Reader) []zincsearch.Searcher {
	searchReaders := make([]zincsearch.Searcher, len(readers))
	for i, r := range readers {
		searchReaders[i] = r
	}
	return searchReaders
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

// QueryStatsInfo get statistics from specified indexes, returns statistics for all index mergers.
func QueryStatsInfo(indexNames []string, query *meta.ZincQuery) (*zincsearch.UnifiedStats, error) {
	hasIndex := false
	searchIndex := make([]*Index, 0)
	var mappings *meta.Mappings
	var analyzers map[string]*analysis.Analyzer

	for _, indexName := range indexNames {
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
	var searchReaders []*bluge.Reader

	mutex := sync.Mutex{}
	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.LoadObjGoroutineNum)
	for _, index := range searchIndex {
		index := index
		eg.Go(func() error {
			reader, err := index.GetReaders(0, 0)
			if err != nil {
				return err
			}
			mutex.Lock()
			searchReaders = append(searchReaders, reader...)
			if mappings == nil {
				mappings = index.GetMappings()
				analyzers = index.GetAnalyzers()
			}
			mutex.Unlock()
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return nil, err
	}
	if len(searchReaders) == 0 {
		if !hasIndex {
			return nil, fmt.Errorf("core.UnifySearch: error accessing reader: no index found")
		}
		return nil, nil
	}
	defer func() {
		for _, readers := range searchReaders {
			_ = readers.Close()
		}
	}()

	result := &zincsearch.UnifiedStats{
		FieldStats: map[string]*zincsearch.FieldStats{},
	}

	// field -> []terms
	termMap := make(map[string][]query2.Term)
	// we assume that all indexes have same mappings and analyzers.
	termList, err := query2.QueryTerms(query.Query, mappings, analyzers)
	if err != nil {
		return nil, err
	}
	for _, term := range termList {
		if tms, ok := termMap[term.Field]; ok {
			termMap[term.Field] = append(tms, term)
		} else {
			termMap[term.Field] = []query2.Term{term}
		}
	}

	for field, terms := range termMap {
		for _, reader := range searchReaders {
			fieldStats, err := getFieldStats(reader, field, terms)
			if err != nil {
				return nil, err
			}
			result.MergeField(fieldStats)
		}
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
