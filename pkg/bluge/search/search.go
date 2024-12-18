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

package search

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"

	"golang.org/x/sync/errgroup"

	"github.com/blugelabs/bluge/search/highlight"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery"
	"github.com/zincsearch/zincsearch/pkg/uquery/fields"
	"github.com/zincsearch/zincsearch/pkg/uquery/source"
)

// ReaderOpener
// used for lazy load readers
// it won't load reader until GetReader() be called
type ReaderOpener interface {
	GetReader() (Searcher, error)
	GetId() int64
}

func MultiSearch(
	ctx context.Context,
	query *meta.ZincQuery,
	mappings *meta.Mappings,
	analyzers map[string]*analysis.Analyzer,
	shardNum int64,
	readerOpeners []ReaderOpener,
) (*meta.SearchResponse, error) {
	if len(readerOpeners) == 0 {
		return &meta.SearchResponse{
			Hits: meta.Hits{
				Hits: []meta.Hit{},
				Total: meta.Total{
					Value: 0,
				}},
			Took:   0,
			Shards: meta.Shards{Total: shardNum, Successful: int64(0), Skipped: shardNum - int64(len(readerOpeners))},
		}, nil
	}
	// for single reader, we just get result directly.
	if len(readerOpeners) == 1 {
		reader, err := readerOpeners[0].GetReader()
		if err != nil {
			return nil, fmt.Errorf("open reader err: %w", err)
		}
		// there is not any document
		if reader == nil {
			return &meta.SearchResponse{
				Hits: meta.Hits{
					Hits: []meta.Hit{},
					Total: meta.Total{
						Value: 0,
					}},
				Took:   0,
				Shards: meta.Shards{Total: shardNum, Successful: int64(0), Skipped: shardNum - int64(len(readerOpeners))},
			}, nil
		}
		return getSingleReaderResult(ctx, query, mappings, analyzers, shardNum, reader)
	}

	// for multi-reader we need to use heap sort to combine all the results,
	// and for each reader, it needs to be closed immediately after it completes the search to release resources.
	bucketAggs := make(map[string]search.Aggregation)
	bucketAggs["duration"] = aggregations.Duration()

	eg := &errgroup.Group{}
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	docs := make(chan *Document, len(readerOpeners)*10)
	aggs := make(chan *search.Bucket, len(readerOpeners))

	docList := &DocumentList{
		bucket: search.NewBucket("", bucketAggs),
		from:   int64(query.From),
		size:   int64(query.Size),
	}
	heap.Init(docList)
	// handle skip and limit
	maxSize := int64(query.Size)
	query.Size += query.From
	query.From = 0

	egDoc := &errgroup.Group{}
	egDoc.Go(func() error {
		for doc := range docs {
			heap.Push(docList, doc)
		}
		return nil
	})
	egDoc.Go(func() error {
		for agg := range aggs {
			docList.bucket.Merge(agg)
		}
		return nil
	})

	err := uquery.NormalizeQuery(query, mappings, analyzers)
	if err != nil {
		return nil, err
	}
	req, err := uquery.ParseQueryDSL(query, mappings, analyzers)
	if err != nil {
		return nil, err
	}
	if docList.sort == nil {
		if req, ok := req.(*bluge.TopNSearch); ok {
			docList.sort = req.SortOrder().Copy()
		}
	}

	openedMutex := sync.Mutex{}
	opened := make([]Searcher, 0)
	defer func() {
		for _, r := range opened {
			_ = r.Close()
		}
	}()

	for _, opener := range readerOpeners {
		// A new request is created for each reader. Query is not modified concurrently.
		req, err := uquery.ParseQueryDSL(query, mappings, analyzers)
		if err != nil {
			return nil, err
		}
		opener := opener
		eg.Go(func() error {
			r, err := opener.GetReader()
			if err != nil {
				return fmt.Errorf("open reader err: %w", err)
			}
			// new shard without any document
			if r == nil {
				return nil
			}
			defer func() {
				_ = r.Close()
			}()
			openedMutex.Lock()
			opened = append(opened, r)
			openedMutex.Unlock()
			var n int64
			dmi, err := r.Search(ctx, req)
			if err != nil {
				return err
			}
			next, err := dmi.Next()
			for err == nil && next != nil {
				n++
				hit, err2 := visitMatchedDoc(next, query, mappings)
				if err2 != nil {
					return err2
				}
				docs <- &Document{hit: &hit, match: next}
				next, err = dmi.Next()
			}
			aggs <- dmi.Aggregations()

			if n > atomic.LoadInt64(&docList.size) {
				atomic.StoreInt64(&docList.size, n)
			}

			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	close(docs)
	close(aggs)
	_ = egDoc.Wait()

	docList.Done()
	docList.bucket.Aggregation("duration").Finish()

	if docList.size > maxSize {
		docList.size = maxSize
	}

	resp := &meta.SearchResponse{
		Hits: meta.Hits{Hits: []meta.Hit{}},
	}

	hit, err := docList.Next()
	for err == nil && hit != nil {
		resp.Hits.Hits = append(resp.Hits.Hits, *hit)
		hit, err = docList.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("search.MultiSearch: error iterating results: %w", err)
	}
	resp.Took = int(docList.Aggregations().Duration().Milliseconds())
	resp.Shards = meta.Shards{Total: shardNum, Successful: int64(len(readerOpeners)), Skipped: shardNum - int64(len(readerOpeners))}
	resp.Hits.Total = meta.Total{Value: int(docList.Aggregations().Count())}
	resp.Hits.MaxScore = docList.Aggregations().Metric("max_score")
	if err := uquery.FormatResponse(resp, query, docList.Aggregations()); err != nil {
		log.Error().Msgf("search.MultiSearch: error format response: %s", err.Error())
	}
	return resp, nil
}

func getSingleReaderResult(ctx context.Context,
	query *meta.ZincQuery,
	mappings *meta.Mappings,
	analyzers map[string]*analysis.Analyzer,
	shardNum int64, reader Searcher) (*meta.SearchResponse, error) {
	defer func() {
		_ = reader.Close()
	}()
	err := uquery.NormalizeQuery(query, mappings, analyzers)
	if err != nil {
		return nil, err
	}
	req, err := uquery.ParseQueryDSL(query, mappings, analyzers)
	if err != nil {
		return nil, err
	}
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, err
	}
	resp := &meta.SearchResponse{
		Hits: meta.Hits{Hits: []meta.Hit{}},
	}

	next, err := dmi.Next()
	for err == nil && next != nil {
		hit, err2 := visitMatchedDoc(next, query, mappings)
		if err2 != nil {
			err = err2
			break
		}
		resp.Hits.Hits = append(resp.Hits.Hits, hit)
		next, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("search.MultiSearch: error iterating results: %w", err)
	}
	resp.Took = int(dmi.Aggregations().Duration().Milliseconds())
	resp.Shards = meta.Shards{Total: shardNum, Successful: int64(1), Skipped: shardNum - 1}
	resp.Hits.Total = meta.Total{Value: int(dmi.Aggregations().Count())}
	resp.Hits.MaxScore = dmi.Aggregations().Metric("max_score")
	if err := uquery.FormatResponse(resp, query, dmi.Aggregations()); err != nil {
		log.Error().Msgf("search.MultiSearch: error format response: %s", err.Error())
	}

	return resp, nil
}

// visitMatchedDoc
// visit matched document's all fields, this must be called before reader close.
func visitMatchedDoc(next *search.DocumentMatch, query *meta.ZincQuery, mappings *meta.Mappings) (meta.Hit, error) {
	// highlight
	var highlighter *highlight.SimpleHighlighter
	if query.Highlight != nil {
		if len(query.Highlight.PreTags) > 0 && len(query.Highlight.PostTags) > 0 {
			highlighter = highlight.NewHTMLHighlighterTags(query.Highlight.PreTags[0], query.Highlight.PostTags[0])
		} else {
			highlighter = highlight.NewHTMLHighlighter()
		}
	}

	var id string
	var indexName string

	var sourceData map[string]interface{}
	var fieldsData map[string]interface{}
	var highlightData map[string]interface{}
	if query.Highlight != nil {
		highlightData = make(map[string]interface{})
	}
	err := next.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case "_id":
			id = string(value)
		case "_index":
			indexName = string(value)
		case "_source":
			sourceData = source.Response(query.Source.(*meta.Source), value)
			if query.Fields != nil {
				fieldsData = fields.Response(query.Fields.([]*meta.Field), value, mappings)
			}
		default:
			// highlight
			if query.Highlight != nil && query.Highlight.Fields != nil {
				if options, ok := query.Highlight.Fields[field]; ok {
					if v, ok := next.Locations[field]; ok {
						if len(options.PreTags) > 0 && len(options.PostTags) > 0 {
							highlighter := highlight.NewHTMLHighlighterTags(options.PreTags[0], options.PostTags[0])
							highlightData[field] = highlighter.BestFragments(v, value, options.NumberOfFragments)
						} else {
							highlightData[field] = highlighter.BestFragments(v, value, options.NumberOfFragments)
						}
					}
				}
			}
		}

		return true
	})
	if err != nil {
		return meta.Hit{}, fmt.Errorf("search.MultiSearch: error accessing stored fields: %w", err)
	}

	hit := meta.Hit{
		Index:     indexName,
		Type:      "_doc",
		ID:        id,
		Score:     next.Score,
		Source:    sourceData,
		Fields:    fieldsData,
		Highlight: highlightData,
	}
	if query.Explain {
		hit.Explain = next.Explanation
	}

	return hit, nil
}

type Document struct {
	hit   *meta.Hit
	match *search.DocumentMatch
}

type DocumentList struct {
	from   int64
	size   int64
	len    int64
	next   int64
	docs   []*Document
	bucket *search.Bucket
	sort   search.SortOrder
}

func (d *DocumentList) Done() {
	// do skip
	alldocLen := int64(d.Len())
	for i := int64(0); i < d.from && i < alldocLen; i++ {
		heap.Pop(d)
	}
	// log size
	d.len = int64(len(d.docs))
}

func (d *DocumentList) Next() (*meta.Hit, error) {
	if d.next >= d.size || d.next >= d.len {
		return nil, nil
	}
	doc := heap.Pop(d)
	d.next++
	return doc.(*Document).hit, nil
}

func (d *DocumentList) Aggregations() *search.Bucket {
	return d.bucket
}

func (d *DocumentList) Push(doc interface{}) {
	d.docs = append(d.docs, doc.(*Document))
}

func (d *DocumentList) Pop() interface{} {
	n := len(d.docs)
	doc := d.docs[n-1]
	d.docs = d.docs[:n-1]
	return doc
}

func (d *DocumentList) Len() int { return len(d.docs) }
func (d *DocumentList) Less(i, j int) bool {
	return d.sort.Compare(d.docs[i].match, d.docs[j].match) < 0
}
func (d *DocumentList) Swap(i, j int) { d.docs[i], d.docs[j] = d.docs[j], d.docs[i] }
