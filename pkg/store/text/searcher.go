package text

import (
	"context"

	"github.com/zincsearch/zincsearch/pkg/meta"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/rs/zerolog/log"
)

// SimpleSearcher lazily opens a request Reader. It is for sequential use within
// one request. Close it after consuming results and before releasing the owning
// store Index reference. A borrowed Reader must not be closed separately.
type SimpleSearcher struct {
	shard         *IndexShard
	secondShardID int64
	reader        *bluge.Reader
	opened        bool
	closed        bool
}

func (s *SimpleSearcher) Search(ctx context.Context, req bluge.SearchRequest) (search.DocumentMatchIterator, error) {
	reader, err := s.getReader(ctx)
	if err != nil {
		s.Close()
		return nil, err
	}
	if reader == nil {
		return &emptyDocumentMatchIterator{}, nil
	}
	iterator, err := reader.Search(ctx, req)
	if err != nil {
		s.Close()
		return nil, err
	}
	return iterator, nil
}

func (s *SimpleSearcher) GetReader() (*bluge.Reader, error) {
	return s.getReader(context.Background())
}

func (s *SimpleSearcher) getReader(ctx context.Context) (*bluge.Reader, error) {
	if s.closed {
		return nil, ErrClosed
	}
	err := ctx.Err()
	if err != nil {
		return nil, err
	}
	if !s.opened {
		reader, err := s.shard.openReader(ctx, s.secondShardID)
		if err != nil {
			return nil, err
		}
		s.reader = reader
		s.opened = true
	}
	err = ctx.Err()
	if err != nil {
		s.Close()
		return nil, err
	}
	return s.reader, nil
}

// Close releases the Reader once. Cleanup errors are ignored; the error result
// is retained for compatibility with the searcher interface.
func (s *SimpleSearcher) Close() error {
	if !s.closed {
		s.closed = true
		if s.reader != nil {
			s.reader.Close()
			s.reader = nil
		}
	}
	return nil
}

// GetSearchers borrows shard metadata only while selecting runtime shards. The
// caller protects metadata access; returned searchers retain no metadata pointers.
func (index *Index) GetSearchers(shards map[string]*meta.IndexShard, timeMin, timeMax int64) []*SimpleSearcher {
	var searchers []*SimpleSearcher
	for id, shard := range index.shards {
		searchers = append(searchers, shard.GetSearchers(shards[id], timeMin, timeMax)...)
	}
	return searchers
}

// GetSearchersByID follows the same metadata borrowing contract as GetSearchers.
func (index *Index) GetSearchersByID(shards map[string]*meta.IndexShard, timeMin, timeMax int64, ids ...int) []*SimpleSearcher {
	var searchers []*SimpleSearcher
	for id, shard := range index.shards {
		searchers = append(searchers, shard.GetPartialSearchers(shards[id], timeMin, timeMax, ids...)...)
	}
	return searchers
}

// GetSearchers reads borrowed metadata under the caller's metadata protection.
func (s *IndexShard) GetSearchers(ref *meta.IndexShard, timeMin, timeMax int64) []*SimpleSearcher {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var searchers []*SimpleSearcher
	if ref == nil {
		return searchers
	}
	for i := len(s.shards) - 1; i >= 0; i-- {
		if i >= len(ref.Shards) || ref.Shards[i] == nil {
			continue
		}
		second := ref.Shards[i]
		if !matchesTimeRange(second, timeMin, timeMax) {
			continue
		}
		searchers = append(searchers, &SimpleSearcher{shard: s, secondShardID: second.ID})
		if second.Stats.DocTimeMin > 0 && second.Stats.DocTimeMin < timeMin {
			break
		}
	}
	return searchers
}

// GetPartialSearchers borrows metadata under the caller's metadata protection.
func (s *IndexShard) GetPartialSearchers(ref *meta.IndexShard, timeMin, timeMax int64, ids ...int) []*SimpleSearcher {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var searchers []*SimpleSearcher
	if ref == nil {
		return searchers
	}
	for _, id := range ids {
		if id < 0 || id >= len(s.shards) || id >= len(ref.Shards) || ref.Shards[id] == nil {
			log.Warn().Str("shard", s.name).Int("second shard", id).Msg("invalid partial second shard ID")
			continue
		}
		second := ref.Shards[id]
		if !matchesTimeRange(second, timeMin, timeMax) {
			continue
		}
		searchers = append(searchers, &SimpleSearcher{shard: s, secondShardID: second.ID})
	}
	return searchers
}

// The caller protects access to the borrowed metadata.
func matchesTimeRange(second *meta.IndexSecondShard, timeMin, timeMax int64) bool {
	stats := second.Stats
	return (timeMin <= 0 || stats.DocTimeMax <= 0 || stats.DocTimeMax >= timeMin) &&
		(timeMax <= 0 || stats.DocTimeMin <= 0 || stats.DocTimeMin <= timeMax)
}

// emptyDocumentMatchIterator represents a successful search with no matches.
// Read-only queries use it when a shard directory has not been created yet,
// treating the shard as empty. Next signals completion, and Aggregations returns
// an empty bucket so callers can handle it as a normal search result.
type emptyDocumentMatchIterator struct{}

func (e *emptyDocumentMatchIterator) Next() (*search.DocumentMatch, error) {
	return nil, nil
}

func (e *emptyDocumentMatchIterator) Aggregations() *search.Bucket {
	return search.NewBucket("", nil)
}
