package text

import (
	"context"

	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/source"

	"github.com/blugelabs/bluge"
)

// FindDocument returns the newest matching shard and optionally its document.
// It borrows metadata only during the call. Search, stored-field consumption and
// Reader cleanup finish before returning, including all error paths.
func (s *IndexShard) FindDocument(ctx context.Context, ref *meta.IndexShard, docID string, withSource bool) (int64, *meta.Hit, error) {
	searchers := s.GetSearchers(ref, 0, 0)
	for _, searcher := range searchers {
		id, hit, err := findDocument(ctx, searcher, docID, withSource)
		if err != nil || id >= 0 {
			return id, hit, err
		}
	}
	return ShardIDNeedLatest, nil, zincerrors.ErrorIDNotFound
}

func findDocument(ctx context.Context, searcher *SimpleSearcher, docID string, withSource bool) (int64, *meta.Hit, error) {
	id := ShardIDNeedLatest
	defer searcher.Close()
	request := bluge.NewTopNSearch(1, bluge.NewTermQuery(docID).SetField("_id"))
	iterator, err := searcher.Search(ctx, request)
	if err != nil {
		return id, nil, err
	}
	match, err := iterator.Next()
	if err != nil || match == nil {
		return id, nil, err
	}
	id = searcher.secondShardID
	if !withSource {
		return id, nil, nil
	}
	hit := &meta.Hit{ID: docID, Type: "_doc"}
	err = match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case "_id":
			hit.ID = string(value)
		case "_index":
			hit.Index = string(value)
		case "_source":
			hit.Source = source.Response(&meta.Source{Enable: true}, value)
		}
		return true
	})
	return id, hit, err
}
