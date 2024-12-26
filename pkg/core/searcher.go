package core

import (
	"context"
	"encoding/base64"

	"github.com/blugelabs/bluge"
	search1 "github.com/blugelabs/bluge/search"
	segment "github.com/blugelabs/bluge_segment_api"
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

// UnifiedSearcher is an alternative to bluge.Reader.
// It searches in a single bluge.search.Reader object but use statistics from multiple Readers,
// to produce unified scores for documents when multiple indexes are searched.
// The statistics may be collected from multiple nodes if cluster is used.
// It is similar to SimpleSearcher, and it doesn't immediately load bluge.reader,
// but it additionally includes statistical information for unified search.
// It must be closed after using.
type UnifiedSearcher struct {
	shard         *IndexShard
	secondShardID int64
	stats         *search.UnifiedStats
	reader        *unifiedReader
}

func (u *UnifiedSearcher) Search(ctx context.Context, req bluge.SearchRequest) (search1.DocumentMatchIterator, error) {
	collector := req.Collector()
	blugeReader, err := u.shard.openReader(u.secondShardID)
	if err != nil {
		return nil, err
	}
	if blugeReader == nil {
		return &emptyDocumentMatchIterator{}, nil
	}
	config := blugeReader.GetConfig()
	u.reader = &unifiedReader{
		stats:        u.stats,
		activeReader: blugeReader.GetUnderlyingReader(),
	}
	searcher, err := req.Searcher(u.reader, config)
	if err != nil {
		return nil, err
	}

	memNeeded := bluge.MemNeededForSearch(searcher, collector)
	if config.SearchStartFunc != nil {
		err = config.SearchStartFunc(memNeeded)
	}
	if err != nil {
		return nil, err
	}
	if config.SearchEndFunc != nil {
		defer config.SearchEndFunc(memNeeded)
	}

	var dmItr search1.DocumentMatchIterator
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

// unifiedReader
// unified reader is a wrapper for bluge.search.reader and statistics
// to calculate unified scores for searching multiple indexes.
type unifiedReader struct {
	stats        *search.UnifiedStats
	activeReader search1.Reader
}

func (r *unifiedReader) GetDocumentFreq(term []byte, field string, includeFreq, includeNorm, includeTermVectors bool) (uint64, error) {
	if fieldInfo, ok := r.stats.FieldStats[field]; ok {
		if termInfo, ok := fieldInfo.TermStats[base64.StdEncoding.EncodeToString(term)]; ok {
			return termInfo.Freq, nil
		}
	}
	// Some terms may not exist in stats map, these terms should belong to Bool Query's filter parameter.
	// In this case, it's ok that we can directly use the reader to obtain frequency, because it doesn't affect the score.
	return r.activeReader.GetDocumentFreq(term, field, includeFreq, includeNorm, includeTermVectors)
}

func (r *unifiedReader) DocumentValueReader(fields []string) (segment.DocumentValueReader, error) {
	return r.activeReader.DocumentValueReader(fields)
}

func (r *unifiedReader) VisitStoredFields(number uint64, visitor segment.StoredFieldVisitor) error {
	return r.activeReader.VisitStoredFields(number, visitor)
}

func (r *unifiedReader) CollectionStats(field string) (segment.CollectionStats, error) {
	if stats, ok := r.stats.FieldStats[field]; ok {
		return stats, nil
	}
	// Some field may not exist in stats map, these fields should belong to Bool Query's filter parameter.
	// In this case, it's ok that we can directly use the reader to obtain collection stats, because it doesn't affect the score.
	return r.activeReader.CollectionStats(field)
}

func (r *unifiedReader) DictionaryLookup(field string) (segment.DictionaryLookup, error) {
	return r.activeReader.DictionaryLookup(field)
}

func (r *unifiedReader) DictionaryIterator(field string, automaton segment.Automaton, start, end []byte) (segment.DictionaryIterator, error) {
	return r.activeReader.DictionaryIterator(field, automaton, start, end)
}

func (r *unifiedReader) PostingsIterator(term []byte, field string, includeFreq, includeNorm, includeTermVectors bool) (segment.PostingsIterator, error) {
	return r.activeReader.PostingsIterator(term, field, includeFreq, includeNorm, includeTermVectors)
}

func (r *unifiedReader) Close() error {
	return r.activeReader.Close()
}

type emptyDocumentMatchIterator struct {
}

func (e *emptyDocumentMatchIterator) Next() (*search1.DocumentMatch, error) {
	return nil, nil
}

func (e *emptyDocumentMatchIterator) Aggregations() *search1.Bucket {
	return search1.NewBucket("", nil)
}
