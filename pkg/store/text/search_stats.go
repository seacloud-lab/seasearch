package text

import (
	"context"
	"encoding/base64"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	segment "github.com/blugelabs/bluge_segment_api"
)

// CollectStats consumes and closes all searchers, including on error. The caller
// holds the Index reference and metadata protection until this function returns.
func CollectStats(ctx context.Context, searchers []*SimpleSearcher, terms []query.Term) (*zincsearch.UnifiedStats, error) {
	defer func() {
		for _, searcher := range searchers {
			searcher.Close()
		}
	}()
	result := &zincsearch.UnifiedStats{FieldStats: make(map[string]*zincsearch.FieldStats)}
	// Count each term once even when it occurs multiple times in the query.
	fields := make(map[string]map[string][]byte)
	for _, term := range terms {
		if fields[term.Field] == nil {
			fields[term.Field] = make(map[string][]byte)
		}
		fields[term.Field][base64.StdEncoding.EncodeToString(term.Value)] = term.Value
	}
	for _, searcher := range searchers {
		reader, openErr := searcher.getReader(ctx)
		if openErr != nil {
			return nil, openErr
		}
		if reader != nil {
			for field, values := range fields {
				err := ctx.Err()
				if err != nil {
					return nil, err
				}
				stats, err := reader.GetUnderlyingReader().CollectionStats(field)
				if err != nil {
					return nil, err
				}
				fieldStats := &zincsearch.FieldStats{
					Field:     field,
					TermStats: make(map[string]*zincsearch.TermStats),
				}
				fieldStats.Merge(stats)
				for key, value := range values {
					err := ctx.Err()
					if err != nil {
						return nil, err
					}
					freq, err := reader.GetUnderlyingReader().GetDocumentFreq(value, field, true, true, false)
					if err != nil {
						return nil, err
					}
					fieldStats.TermStats[key] = &zincsearch.TermStats{Value: key, Freq: freq}
				}
				result.MergeField(fieldStats)
			}
		}
		searcher.Close()
	}
	return result, ctx.Err()
}

// UnifiedSearcher owns base and borrows immutable statistics for one request.
// Close the wrapper, not base or its Reader, before releasing the Index.
type UnifiedSearcher struct {
	base  *SimpleSearcher
	stats *zincsearch.UnifiedStats
}

func NewUnifiedSearcher(base *SimpleSearcher, stats *zincsearch.UnifiedStats) *UnifiedSearcher {
	return &UnifiedSearcher{base: base, stats: stats}
}

func (s *UnifiedSearcher) Search(ctx context.Context, req bluge.SearchRequest) (iterator search.DocumentMatchIterator, err error) {
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	reader, err := s.base.getReader(ctx)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return &emptyDocumentMatchIterator{}, nil
	}
	if s.stats == nil {
		return reader.Search(ctx, req)
	}
	cfg := reader.GetConfig()
	underlying := &unifiedReader{Reader: reader.GetUnderlyingReader(), stats: s.stats}
	searcher, err := req.Searcher(underlying, cfg)
	if err != nil {
		return nil, err
	}
	collector := req.Collector()
	memNeeded := bluge.MemNeededForSearch(searcher, collector)
	if cfg.SearchStartFunc != nil {
		err = cfg.SearchStartFunc(memNeeded)
		if err != nil {
			return nil, err
		}
	}
	if cfg.SearchEndFunc != nil {
		defer cfg.SearchEndFunc(memNeeded)
	}
	return collector.Collect(ctx, req.Aggregations(), searcher)
}

func (s *UnifiedSearcher) Close() error {
	return s.base.Close()
}

// unifiedReader borrows the underlying Reader; base remains its sole owner.
type unifiedReader struct {
	search.Reader
	stats *zincsearch.UnifiedStats
}

func (r *unifiedReader) GetDocumentFreq(term []byte, field string, includeFreq, includeNorm, includeTermVectors bool) (uint64, error) {
	if fieldStats := r.stats.FieldStats[field]; fieldStats != nil {
		if termStats := fieldStats.TermStats[base64.StdEncoding.EncodeToString(term)]; termStats != nil {
			return termStats.Freq, nil
		}
	}
	return r.Reader.GetDocumentFreq(term, field, includeFreq, includeNorm, includeTermVectors)
}

func (r *unifiedReader) CollectionStats(field string) (segment.CollectionStats, error) {
	if stats := r.stats.FieldStats[field]; stats != nil {
		return stats, nil
	}
	return r.Reader.CollectionStats(field)
}

func (r *unifiedReader) Close() error {
	return nil // The SimpleSearcher closes the owning bluge.Reader.
}
