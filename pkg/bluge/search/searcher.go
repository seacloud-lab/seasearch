package search

import (
	"context"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
)

// Searcher
// is an interface for easy to search with bluge.reader or unified_searcher in a consistent way
type Searcher interface {
	Search(ctx context.Context, req bluge.SearchRequest) (search.DocumentMatchIterator, error)
	Close() error
}

// UnifiedSearcher is an alternative to bluge.Reader.
// It searches in a single bluge.search.Reader object but use statistics from multiple Readers,
// to produce unified scores for documents when multiple indexes are searched.
// The statistics may be collected from multiple nodes if cluster is used.
type UnifiedSearcher struct {
	reader search.Reader
	config bluge.Config
}

func NewUnifiedSearcher(reader *bluge.Reader, stats *UnifiedStats) *UnifiedSearcher {
	r := &unifiedReader{
		stats:        stats,
		activeReader: reader.GetUnderlyingReader(),
	}
	return &UnifiedSearcher{
		reader: r,
		config: reader.GetConfig(),
	}
}

func (u *UnifiedSearcher) Search(ctx context.Context, req bluge.SearchRequest) (search.DocumentMatchIterator, error) {
	collector := req.Collector()
	searcher, err := req.Searcher(u.reader, u.config)
	if err != nil {
		return nil, err
	}

	memNeeded := bluge.MemNeededForSearch(searcher, collector)
	if u.config.SearchStartFunc != nil {
		err = u.config.SearchStartFunc(memNeeded)
	}
	if err != nil {
		return nil, err
	}
	if u.config.SearchEndFunc != nil {
		defer u.config.SearchEndFunc(memNeeded)
	}

	var dmItr search.DocumentMatchIterator
	dmItr, err = collector.Collect(ctx, req.Aggregations(), searcher)
	if err != nil {
		return nil, err
	}

	return dmItr, nil
}

func (u *UnifiedSearcher) Close() error {
	if u.reader == nil {
		return nil
	}
	return u.reader.Close()
}
