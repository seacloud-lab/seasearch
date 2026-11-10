package text

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	segment "github.com/blugelabs/bluge_segment_api"
	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
)

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
			err = errors.Join(err, s.Close())
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
		if err := cfg.SearchStartFunc(memNeeded); err != nil {
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
