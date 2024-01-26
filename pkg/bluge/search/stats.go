package search

import segment "github.com/blugelabs/bluge_segment_api"

type UnifiedStats struct {
	FieldStats map[string]*FieldStats `json:"collection_stats"` // field -> stats
}

func (s *UnifiedStats) Merge(other *UnifiedStats) {
	for _, fieldStats := range other.FieldStats {
		s.MergeField(fieldStats)
	}
}

func (s *UnifiedStats) MergeField(f *FieldStats) {
	if s.FieldStats == nil {
		s.FieldStats = make(map[string]*FieldStats)
	}
	if fieldStats, ok := s.FieldStats[f.Field]; ok {
		fieldStats.TotalDocCount += f.TotalDocCount
		fieldStats.DocCount += f.DocCount
		fieldStats.SumTotalTermFreq += f.SumTotalTermFreq
	} else {
		s.FieldStats[f.Field] = &FieldStats{
			Field:            f.Field,
			DocCount:         f.DocCount,
			TotalDocCount:    f.TotalDocCount,
			SumTotalTermFreq: f.SumTotalTermFreq,
		}
	}

	for _, termStats := range f.TermStats {
		s.MergeTerm(f.Field, termStats.Value, termStats.Freq)
	}
}

// MergeTerm
// the term value should be encoded with base64
func (s *UnifiedStats) MergeTerm(field string, term string, freq uint64) {
	if s.FieldStats == nil {
		s.FieldStats = make(map[string]*FieldStats)
	}
	if fieldStats, ok := s.FieldStats[field]; ok {
		if fieldStats.TermStats == nil {
			fieldStats.TermStats = make(map[string]*TermStats)
		}
		if termStats, ok := fieldStats.TermStats[term]; ok {
			termStats.Freq += freq
		} else {
			fieldStats.TermStats[term] = &TermStats{
				Freq:  freq,
				Value: term,
			}
		}
	} else {
		s.FieldStats[field] = &FieldStats{
			TermStats: map[string]*TermStats{
				term: {
					Freq:  freq,
					Value: term,
				},
			},
			Field: field,
		}
	}
}

// FieldStats implemented segment.CollectionStats interface
type FieldStats struct {
	TermStats        map[string]*TermStats
	Field            string `json:"field"`
	SumTotalTermFreq uint64 `json:"sum_total_term_frequency"`
	DocCount         uint64 `json:"document_count"`
	TotalDocCount    uint64 `json:"total_document_count"`
}

type TermStats struct {
	Value string `json:"value"` // Value is encoded with base64
	Freq  uint64 `json:"freq"`
}

func (m *FieldStats) TotalDocumentCount() uint64 {
	return m.TotalDocCount
}

func (m *FieldStats) DocumentCount() uint64 {
	return m.DocCount
}

func (m *FieldStats) SumTotalTermFrequency() uint64 {
	return m.SumTotalTermFreq
}

func (m *FieldStats) Merge(other segment.CollectionStats) {
	// When index is empty, the CollectionStats returned by bluge.Reader is nil.
	if other == nil {
		return
	}
	m.TotalDocCount += other.TotalDocumentCount()
	m.DocCount += other.DocumentCount()
	m.SumTotalTermFreq += other.SumTotalTermFrequency()
}
