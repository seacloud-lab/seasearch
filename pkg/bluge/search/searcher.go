package search

import (
	"context"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
)

// Searcher interface is an additional layer of abstraction on top of bluge.Reader. Currently 3 kinds of objects implements it:
// - bluge.Reader
// - SimpleSearcher: only opens the underlying bluge.Reader object when Search() is called.
// - UnifiedSearcher: use stats from multiple bluge.Readers to produce unified scores for documents. It also only opens the underlying readers when Search() is called.
type Searcher interface {
	Search(ctx context.Context, req bluge.SearchRequest) (search.DocumentMatchIterator, error)
	Close() error
}
