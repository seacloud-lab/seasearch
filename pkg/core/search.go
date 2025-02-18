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
	"math"
	"sort"
	"sync"
	"time"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery"
	"github.com/zincsearch/zincsearch/pkg/uquery/timerange"
	"golang.org/x/sync/errgroup"
)

func (index *Index) Search(query *meta.ZincQuery) (*meta.SearchResponse, error) {
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
	searchers := index.GetSearchers(timeMin, timeMax)
	ctx := context.Background()
	var cancel context.CancelFunc
	if query.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(query.Timeout)*time.Second)
		defer cancel()
	}

	// dmi, err := bluge.MultiSearch(ctx, searchRequest, readers...)
	return zincsearch.MultiSearch(ctx, query, mappings, analyzers, index.GetAllShardNum(), simpleSearchersToSearcher(searchers)...)
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

func UnifySearchMultiIndex(indexes []IndexQueryRequest, stats *zincsearch.UnifiedStats) (*meta.SearchResponse, error) {
	var query *meta.ZincQuery
	for _, q := range indexes {
		query = q.Query
		break
	}

	var result = &meta.SearchResponse{}
	var mutex = sync.Mutex{}

	var eg errgroup.Group
	eg.SetLimit(config.Global.Shard.LoadObjGoroutineNum)
	for _, indexReq := range indexes {
		index, err := GetZincIndexFromMetadata(indexReq.Index)
		if err != nil {
			return nil, err
		}
		secondIds := indexReq.SecondShardId
		q := indexReq.Query
		eg.Go(func() error {
			res, err := index.PartialSearch(secondIds, q, stats)
			if err != nil {
				return err
			}
			mutex.Lock()
			result.Hits.Total.Value += res.Hits.Total.Value
			result.Hits.MaxScore = math.Max(result.Hits.MaxScore, res.Hits.MaxScore)
			result.Hits.Hits = append(result.Hits.Hits, res.Hits.Hits...)
			result.Took = res.Took
			result.Shards.Total += res.Shards.Total
			result.Shards.Skipped += res.Shards.Skipped
			result.Shards.Failed += res.Shards.Failed
			result.Shards.Successful += res.Shards.Successful
			mutex.Unlock()
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	sort.Slice(result.Hits.Hits, func(i, j int) bool {
		return result.Hits.Hits[i].Score > result.Hits.Hits[j].Score
	})
	if len(result.Hits.Hits) > query.Size {
		result.Hits.Hits = result.Hits.Hits[:query.Size]
	}

	return result, nil
}

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

	return zincsearch.MultiSearch(ctx, query, mappings, analyzers, int64(len(searchers)), simpleSearchersToUnifiedSearcher(stats, searchers)...)
}
