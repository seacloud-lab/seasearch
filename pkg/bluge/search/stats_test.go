package search

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeField(t *testing.T) {
	stats := &UnifiedStats{}
	a1 := &FieldStats{
		Field:            "a",
		TotalDocCount:    1,
		DocCount:         1,
		SumTotalTermFreq: 1,
	}
	a2 := &FieldStats{
		Field:            "a",
		TotalDocCount:    3,
		DocCount:         4,
		SumTotalTermFreq: 5,
	}
	b1 := &FieldStats{
		Field:            "b",
		TotalDocCount:    5,
		DocCount:         5,
		SumTotalTermFreq: 5,
	}
	stats.MergeField(a1)
	stats.MergeField(a2)
	stats.MergeField(b1)

	assert.Equal(t, 2, len(stats.FieldStats))
	assert.Equal(t, uint64(4), stats.FieldStats["a"].TotalDocCount)
	assert.Equal(t, uint64(5), stats.FieldStats["a"].DocCount)
	assert.Equal(t, uint64(6), stats.FieldStats["a"].SumTotalTermFreq)

	assert.Equal(t, uint64(5), stats.FieldStats["b"].TotalDocCount)
	assert.Equal(t, uint64(5), stats.FieldStats["b"].DocCount)
	assert.Equal(t, uint64(5), stats.FieldStats["b"].SumTotalTermFreq)
}

func TestMergeTerm(t *testing.T) {
	stats := &UnifiedStats{}
	a1 := &TermStats{
		Value: "a",
		Freq:  1,
	}
	a2 := &TermStats{
		Value: "a",
		Freq:  1,
	}
	stats.MergeTerm("f1", a1.Value, a1.Freq)
	stats.MergeTerm("f1", a2.Value, a2.Freq)

	assert.Equal(t, 1, len(stats.FieldStats))
	assert.Equal(t, 1, len(stats.FieldStats["f1"].TermStats))

	assert.Equal(t, uint64(2), stats.FieldStats["f1"].TermStats["a"].Freq)

	b1 := &TermStats{
		Value: "b",
		Freq:  3,
	}
	stats.MergeTerm("f2", b1.Value, b1.Freq)
	assert.Equal(t, 2, len(stats.FieldStats))
	assert.Equal(t, 1, len(stats.FieldStats["f1"].TermStats))
	assert.Equal(t, 1, len(stats.FieldStats["f2"].TermStats))

	assert.Equal(t, uint64(3), stats.FieldStats["f2"].TermStats["b"].Freq)

	a3 := &TermStats{
		Value: "c",
		Freq:  5,
	}
	stats.MergeTerm("f1", a3.Value, a3.Freq)
	assert.Equal(t, 2, len(stats.FieldStats))
	assert.Equal(t, 2, len(stats.FieldStats["f1"].TermStats))

	assert.Equal(t, uint64(2), stats.FieldStats["f1"].TermStats["a"].Freq)
	assert.Equal(t, uint64(5), stats.FieldStats["f1"].TermStats["c"].Freq)

}

func TestMerge(t *testing.T) {
	stats := &UnifiedStats{
		FieldStats: map[string]*FieldStats{
			"f1": {
				TermStats: map[string]*TermStats{
					"a1": {
						Value: "a1",
						Freq:  1,
					},
					"b1": {
						Value: "b1",
						Freq:  2,
					},
				},
				TotalDocCount:    10,
				SumTotalTermFreq: 9,
				DocCount:         3,
				Field:            "f1",
			},
			"f2": {
				TermStats: map[string]*TermStats{
					"a2": {
						Value: "a2",
						Freq:  3,
					},
					"b2": {
						Value: "b2",
						Freq:  4,
					},
				},
				TotalDocCount:    1,
				SumTotalTermFreq: 1,
				DocCount:         1,
				Field:            "f2",
			},
		},
	}
	other := &UnifiedStats{
		FieldStats: map[string]*FieldStats{
			"f1": {
				TermStats: map[string]*TermStats{
					"a1": {
						Value: "a1",
						Freq:  2,
					},
					"b1": {
						Value: "b1",
						Freq:  3,
					},
					"c1": {
						Value: "c1",
						Freq:  9,
					},
				},
				TotalDocCount:    2,
				SumTotalTermFreq: 4,
				DocCount:         5,
				Field:            "f1",
			},
			"f3": {
				TermStats: map[string]*TermStats{
					"a3": {
						Value: "a3",
						Freq:  8,
					},
					"b3": {
						Value: "b3",
						Freq:  9,
					},
				},
				TotalDocCount:    2,
				SumTotalTermFreq: 2,
				DocCount:         2,
				Field:            "f3",
			},
		},
	}

	stats.Merge(other)

	assert.Equal(t, 3, len(stats.FieldStats))
	assert.Equal(t, 3, len(stats.FieldStats["f1"].TermStats))

	assert.Equal(t, 3, len(stats.FieldStats["f1"].TermStats))
	assert.Equal(t, 2, len(stats.FieldStats["f2"].TermStats))
	assert.Equal(t, 2, len(stats.FieldStats["f3"].TermStats))

	assert.Equal(t, uint64(12), stats.FieldStats["f1"].TotalDocCount)
	assert.Equal(t, uint64(13), stats.FieldStats["f1"].SumTotalTermFreq)
	assert.Equal(t, uint64(8), stats.FieldStats["f1"].DocCount)

	assert.Equal(t, uint64(1), stats.FieldStats["f2"].TotalDocCount)
	assert.Equal(t, uint64(1), stats.FieldStats["f2"].SumTotalTermFreq)
	assert.Equal(t, uint64(1), stats.FieldStats["f2"].DocCount)

	assert.Equal(t, uint64(2), stats.FieldStats["f3"].TotalDocCount)
	assert.Equal(t, uint64(2), stats.FieldStats["f3"].SumTotalTermFreq)
	assert.Equal(t, uint64(2), stats.FieldStats["f3"].DocCount)

	assert.Equal(t, uint64(3), stats.FieldStats["f1"].TermStats["a1"].Freq)
	assert.Equal(t, uint64(5), stats.FieldStats["f1"].TermStats["b1"].Freq)
	assert.Equal(t, uint64(9), stats.FieldStats["f1"].TermStats["c1"].Freq)

	assert.Equal(t, uint64(3), stats.FieldStats["f2"].TermStats["a2"].Freq)
	assert.Equal(t, uint64(4), stats.FieldStats["f2"].TermStats["b2"].Freq)

	assert.Equal(t, uint64(8), stats.FieldStats["f3"].TermStats["a3"].Freq)
	assert.Equal(t, uint64(9), stats.FieldStats["f3"].TermStats["b3"].Freq)
}
