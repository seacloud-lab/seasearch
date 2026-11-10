package text

import (
	"context"
	"encoding/base64"
	"errors"

	zincsearch "github.com/zincsearch/zincsearch/pkg/bluge/search"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"
)

// CollectStats consumes and closes all searchers, including on error. The caller
// holds the Index reference and metadata protection until this function returns.
func CollectStats(ctx context.Context, searchers []*SimpleSearcher, terms []query.Term) (result *zincsearch.UnifiedStats, err error) {
	defer func() {
		for _, searcher := range searchers {
			if closeErr := searcher.Close(); closeErr != nil && !errors.Is(err, closeErr) {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	result = &zincsearch.UnifiedStats{FieldStats: make(map[string]*zincsearch.FieldStats)}
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
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				stats, err := reader.GetUnderlyingReader().CollectionStats(field)
				if err != nil {
					return nil, err
				}
				fieldStats := &zincsearch.FieldStats{Field: field, TermStats: make(map[string]*zincsearch.TermStats)}
				fieldStats.Merge(stats)
				for key, value := range values {
					if err := ctx.Err(); err != nil {
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
		if err := searcher.Close(); err != nil {
			return nil, err
		}
	}
	return result, ctx.Err()
}
