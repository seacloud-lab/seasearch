package core

import (
	"context"

	"github.com/blugelabs/bluge"
	search1 "github.com/blugelabs/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/bluge/search"
)

// SimpleSearcher
// It won't immediately load bluge.reader, as this requires immediate loading of the underlying files.
// It will only acquire bluge.reader when it starts searching or when GetReader() is called,
// which can avoid opening multiple readers simultaneously and occupying too many resources.
// It must be closed after using.
type SimpleSearcher struct {
	shard         *IndexShard
	secondShardID int64
	reader        *bluge.Reader
}

func (s *SimpleSearcher) Search(ctx context.Context, req bluge.SearchRequest) (search1.DocumentMatchIterator, error) {
	var err error
	if s.reader == nil {
		s.reader, err = s.shard.openReader(s.secondShardID)
		if err != nil {
			return nil, err
		}
		if s.reader == nil {
			return &emptyDocumentMatchIterator{}, nil
		}
	}

	return s.reader.Search(ctx, req)
}

func (s *SimpleSearcher) GetReader() (*bluge.Reader, error) {
	var err error
	if s.reader == nil {
		s.reader, err = s.shard.openReader(s.secondShardID)
		if err != nil {
			return nil, err
		}
	}
	return s.reader, nil
}

func (s *SimpleSearcher) Close() error {
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

// UnifiedSearcher
// It is similar to SimpleSearcher, and it doesn't immediately load bluge.reader,
// but it additionally includes statistical information for unified search.
// It must be closed after using.
type UnifiedSearcher struct {
	shard         *IndexShard
	secondShardID int64
	stats         *search.UnifiedStats
	searcher      *search.UnifiedSearcher
}

func (u *UnifiedSearcher) Search(ctx context.Context, req bluge.SearchRequest) (search1.DocumentMatchIterator, error) {
	if u.searcher == nil {
		blugeReader, err := u.shard.openReader(u.secondShardID)
		if err != nil {
			return nil, err
		}
		if blugeReader == nil {
			return &emptyDocumentMatchIterator{}, nil
		}
		u.searcher = search.NewUnifiedSearcher(blugeReader, u.stats)
	}
	return u.searcher.Search(ctx, req)
}

func (u *UnifiedSearcher) Close() error {
	if u.searcher != nil {
		return u.searcher.Close()
	}
	return nil
}

type emptyDocumentMatchIterator struct {
}

func (e *emptyDocumentMatchIterator) Next() (*search1.DocumentMatch, error) {
	return nil, nil
}

func (e *emptyDocumentMatchIterator) Aggregations() *search1.Bucket {
	return search1.NewBucket("", nil)
}
