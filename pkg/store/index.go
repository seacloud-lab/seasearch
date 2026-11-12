package store

import (
	"errors"
	"fmt"
	"path"
	"sync"

	"github.com/zincsearch/zincsearch/pkg/core"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/store/vector"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"

	"github.com/blugelabs/bluge/analysis"
	"github.com/rs/zerolog/log"
)

var (
	ErrIndexNotFound      = fmt.Errorf("index not found: %w", zincerrors.ErrKeyNotFound)
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
	vector    *vector.VecIndexManager
	// Protected by mu; all vector field layouts have been checked for this
	// Index lifetime, independently of persistence success. Reset when vector
	// fields are added, removed or rebuilt.
	vectorLayoutChecked bool

	// mu protects metadata and dirty. Writers hold it exclusively through the
	// entire operation and synchronous save; queries hold its read lock through
	// result consumption and Reader cleanup. Analysis configuration and analyzers
	// are immutable after publication. Never hold mu while draining references.
	mu sync.RWMutex

	// dirty tracks metadata changes not yet successfully persisted. Set it when
	// modifying ref under mu, including before an operation that may partly fail.
	dirty bool

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

// GetMappings returns an independent copy of the current store-owned mappings.
// The caller must hold an Index reference; mutations to the copy are not saved.
func (index *Index) GetMappings() *meta.Mappings {
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.ref.Mappings == nil {
		return nil
	}
	return index.ref.Mappings.DeepClone()
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
	var errs []error
	if index.vector != nil {
		if err := index.vector.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close vector resources: %w", err))
		}
		index.vector = nil
	}
	index.vectorLayoutChecked = false
	if index.text != nil {
		if err := index.text.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close text resources: %w", err))
		}
		index.text = nil
	}
	err := errors.Join(errs...)
	if err != nil {
		log.Error().Err(err).Str("index", index.name).Msg("failed to close index resources")
	}
	return err
}
