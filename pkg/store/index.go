package store

import (
	"fmt"
	"path"
	"sync"

	"github.com/blugelabs/bluge/analysis"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

var (
	ErrIndexNotFound      = fmt.Errorf("index not found: %w", errors.ErrKeyNotFound)
	ErrIndexAlreadyExists = fmt.Errorf("index already exists")
)

// Index is a shared owner runtime index or an independent read-only query view.
// Each acquisition must remain held while using its shards and readers.
type Index struct {
	name string

	// readOnly is immutable. Query views are caller-owned and are not cached or
	// registered for lifecycle draining.
	readOnly bool

	// ref is the authoritative metadata, including text and vector layouts.
	// Components borrow it only for the duration of a synchronized call.
	ref       *meta.Index
	analyzers map[string]*analysis.Analyzer
	text      *text.Index

	// mu protects metadata; queries hold its read lock through result consumption
	// and Reader cleanup. Analysis configuration and analyzers are immutable after
	// publication. saveMu orders persistence.
	// Neither lock may be held while waiting for Index references to drain.
	// Their scope across component operations is defined by the write protocol.
	mu     sync.RWMutex
	saveMu sync.Mutex

	// Retained while existing callers are migrated to the store-owned state.
	core.Index
}

func (index *Index) GetName() string {
	return index.name
}

func (index *Index) GetStoreName() string {
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.ref.StoreWithHash {
		value := zutils.GetHashEncode(index.name)
		return path.Join(value[:2], value[2:])
	}
	return index.name
}

func (index *Index) GetStorageType() string {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.ref.StorageType
}

func (index *Index) GetStats() meta.IndexStat {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.ref.Stats
}

// MarshalJSON serializes store metadata rather than the embedded legacy Index.
func (index *Index) MarshalJSON() ([]byte, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return json.Marshal(index.ref)
}

// Analyzer selects the same field/name analyzer as the analyze API and returns
// a request-owned copy. Filters and the tokenizer are borrowed immutable objects;
// callers may replace them or append filters, but must not mutate the components.
func (index *Index) Analyzer(field, name string) (*analysis.Analyzer, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	if field != "" && name == "" {
		if prop, ok := index.ref.Mappings.GetProperty(field); ok {
			name = prop.SearchAnalyzer
			if name == "" {
				name = prop.Analyzer
			}
		}
	}
	analyzer, err := zincanalysis.QueryAnalyzer(index.analyzers, name)
	if err != nil {
		if name == "" {
			return new(analysis.Analyzer), nil
		}
		return nil, err
	}
	copy := *analyzer
	copy.CharFilters = append([]analysis.CharFilter(nil), analyzer.CharFilters...)
	copy.TokenFilters = append([]analysis.TokenFilter(nil), analyzer.TokenFilters...)
	return &copy, nil
}

// Close releases an owner cache reference or closes an uncached query view.
// Searchers and their Readers must be closed first. Owner resource cleanup is
// performed separately during eviction, deletion or shutdown. Each acquisition
// must be closed exactly once; failed view cleanup can be retried with Close.
func (index *Index) Close() {
	if index.readOnly {
		if err := index.text.Close(); err != nil {
			log.Warn().Err(err).Str("index", index.name).Msg("failed to close query view")
		}
		return
	}
	cache.Release(index.name)
}

// closeResources is final cleanup, not the public reference-release operation.
// All owner references and request Readers must already have been released.
func (index *Index) closeResources() error {
	if index.text == nil {
		return nil
	}
	err := index.text.Close()
	index.text = nil
	if err != nil {
		log.Error().Err(err).Str("index", index.name).Msg("failed to close text index resources")
	}
	return err
}
