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
	"time"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery"
	"github.com/zincsearch/zincsearch/pkg/uquery/timerange"
)

func (index *Index) Search(query *meta.ZincQuery) (*meta.SearchResponse, error) {
	return index.SearchWithStats(query, nil)
}

func (index *Index) SearchWithStats(query *meta.ZincQuery, stats *zincsearch.UnifiedStats) (*meta.SearchResponse, error) {
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

	if stats != nil {
		return zincsearch.MultiSearch(ctx, query, mappings, analyzers, simpleSearchersToUnifiedSearcher(stats, searchers)...)
	}

	// dmi, err := bluge.MultiSearch(ctx, searchRequest, readers...)
	return zincsearch.MultiSearch(ctx, query, mappings, analyzers, simpleSearchersToSearcher(searchers)...)
}
