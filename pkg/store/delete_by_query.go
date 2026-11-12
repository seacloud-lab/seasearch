package store

import "github.com/zincsearch/zincsearch/pkg/meta"

// DeleteByQueryResult distinguishes the query's reported match count from the
// deduplicated IDs selected by its from/size/sort window. A non-nil result with an
// error means deletion was attempted, but its per-document outcome is uncertain.
type DeleteByQueryResult struct {
	Matched int
	IDs     []string
}

// DeleteByQuery deletes only the documents selected by one query window, not all
// matches. The caller must hold an owner Index reference throughout this call.
// The write lock spans query preparation, Reader cleanup, deletion and synchronous
// metadata saving; it never upgrades a read lock or reacquires the Index by name.
// Query/Reader errors return nil and prevent deletion. Once deletion starts, all
// work and save cleanup finish before returning, even if the query timeout expires.
// Deletion/save errors may leave partial changes; they do not imply rollback.
func (index *Index) DeleteByQuery(query *meta.ZincQuery) (*DeleteByQueryResult, error) {
	ctx, cancel, err := queryContext(query)
	if err != nil {
		return nil, err
	}
	defer cancel()
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := index.checkWriteLocked(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := index.partialSearchLocked(ctx, nil, query, nil)
	if err != nil {
		return nil, err
	}

	result := &DeleteByQueryResult{
		Matched: response.Hits.Total.Value,
		IDs:     make([]string, 0, len(response.Hits.Hits)),
	}
	seen := make(map[string]struct{}, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		if _, ok := seen[hit.ID]; ok {
			continue
		}
		seen[hit.ID] = struct{}{}
		result.IDs = append(result.IDs, hit.ID)
	}
	// All query Readers are closed. Do not start deletion if querying or result
	// collection exhausted the timeout. Write operations use their existing
	// synchronous cleanup protocol rather than this query context.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(result.IDs) == 0 {
		return result, nil
	}
	return result, index.deleteDocumentsLocked(result.IDs, true)
}
