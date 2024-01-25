package search

import (
	"encoding/base64"

	"github.com/blugelabs/bluge/search"
	segment "github.com/blugelabs/bluge_segment_api"
)

// unifiedReader
// unified reader is a wrapper for bluge.search.reader and statistics
// to calculate unified scores for searching multiple indexes.
type unifiedReader struct {
	stats        *UnifiedStats
	activeReader search.Reader
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
