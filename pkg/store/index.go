package store

import (
	"fmt"
	"path"
	"sync"

	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/store/vector"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"

	"github.com/blugelabs/bluge/analysis"
)

var (
	ErrIndexNotFound      = fmt.Errorf("index not found: %w", zincerrors.ErrKeyNotFound)
	ErrIndexAlreadyExists = fmt.Errorf("index already exists")
)

// Index is a shared owner runtime index or an independent read-only query view.
// Each acquisition must remain held while using its shards and readers.
// Exported operations acquire mutex as needed. Unexported helpers that access
// mutable state require the caller to hold mutex unless documented otherwise.
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
	// Protected by mutex; all vector field layouts have been checked for this
	// Index lifetime, independently of persistence success. Reset when vector
	// fields are added, removed or rebuilt.
	vectorLayoutChecked bool

	// mutex protects metadata and dirty. Writers hold it exclusively through the
	// entire operation and synchronous save; queries hold its read lock through
	// result consumption and Reader cleanup. Analysis configuration and analyzers
	// are immutable after publication. Never hold mutex while draining references.
	mutex sync.RWMutex

	// dirty tracks metadata changes not yet successfully persisted. Set it when
	// modifying ref under mutex, including before an operation that may partly fail.
	dirty bool
}

func (index *Index) GetName() string {
	return index.name
}

func (index *Index) GetStoreName() string {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	if index.ref.StoreWithHash {
		value := zutils.GetHashEncode(index.name)
		return path.Join(value[:2], value[2:])
	}
	return index.name
}

func (index *Index) GetStorageType() string {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	return index.ref.StorageType
}

func (index *Index) GetStats() meta.IndexStat {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	return index.ref.Stats
}

// GetMappings returns an independent copy of the current store-owned mappings.
// The caller must hold an Index reference; mutations to the copy are not saved.
func (index *Index) GetMappings() *meta.Mappings {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	if index.ref.Mappings == nil {
		return nil
	}
	return index.ref.Mappings.DeepClone()
}

// MarshalJSON serializes store metadata rather than the embedded legacy Index.
func (index *Index) MarshalJSON() ([]byte, error) {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

	return json.Marshal(index.ref)
}

// Analyzer selects the same field/name analyzer as the analyze API and returns
// a request-owned copy. Filters and the tokenizer are borrowed immutable objects;
// callers may replace them or append filters, but must not mutate the components.
func (index *Index) Analyzer(field, name string) (*analysis.Analyzer, error) {
	index.mutex.RLock()
	defer index.mutex.RUnlock()

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
// must be closed exactly once. Resource cleanup is best-effort.
func (index *Index) Close() {
	if index.readOnly {
		index.text.Close()
		return
	}
	cache.Release(index.name)
}

// closeResources is final cleanup, not the public reference-release operation.
// All owner references and request Readers must already have been released.
// Resource owners log cleanup failures without propagating them. Always attempt
// both vector and text cleanup and discard runtime references, even on failure.
// Callers must unpublish owner instances first; cleanup never gates data deletion.
func (index *Index) closeResources() {
	if index.vector != nil {
		index.vector.Close()
		index.vector = nil
	}
	index.vectorLayoutChecked = false
	if index.text != nil {
		index.text.Close()
		index.text = nil
	}
}
