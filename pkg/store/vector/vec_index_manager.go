package vector

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	vectorstore "github.com/zincsearch/zincsearch/pkg/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

var (
	ErrClosed             = errors.New("vector runtime is closed")
	ErrVecIndexCorruption = errors.New("vector index storage does not match metadata")
)

// Options contains immutable, store-owned values. Paths exclude VecPrefix.
type Options struct {
	Name        string
	StoreName   string
	StorageType string
}

type resources struct {
	mu      sync.Mutex
	storage vectorstore.ObjStore
	tmpDir  string
}

// ensure is called by operations, never while holding the manager cache lock.
// Failed initialization may be retried; there are no native resources to publish.
func (r *resources) ensure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.storage != nil {
		return nil
	}
	storage, err := vectorstore.GetVectorStorage()
	if err != nil {
		return err
	}
	tmpDir := path.Join(config.Global.DataPath, vectorstore.VecPrefix, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return err
	}
	r.storage, r.tmpDir = storage, tmpDir
	return nil
}

// VecIndexManager belongs to exactly one owner Index. Every user must hold that
// Index's reference and metadata lock. It never acquires Index references, saves
// metadata, or starts background work. There is no cross-Index runtime cache.
type VecIndexManager struct {
	options   Options
	mu        sync.Mutex
	fields    map[string]VectorIndex
	resources *resources
	closed    bool
}

// NewVecIndexManager allocates only Go objects, including for text-only indexes.
func NewVecIndexManager(options Options) *VecIndexManager {
	return &VecIndexManager{options: options, fields: make(map[string]VectorIndex)}
}

// Get borrows ref read-only and copies immutable configuration. Actual Writer
// and native resource loading happens in operations, after the manager lock is
// released. Layout reconciliation is an explicit write operation, not a Get side
// effect. Callers must not retain the returned runtime beyond their Index use.
func (m *VecIndexManager) Get(field string, ref *meta.VecIndex) (VectorIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if err := validateLayout(ref); err != nil {
		return nil, err
	}
	if m.options.StorageType != config.Global.StorageType {
		return nil, fmt.Errorf("configured storage backend is %s, index uses %s", config.Global.StorageType, m.options.StorageType)
	}
	cfg := configFromMeta(ref)
	if runtime := m.fields[field]; runtime != nil {
		if runtime.config() != cfg {
			return nil, fmt.Errorf("vector field %s configuration changed", field)
		}
		return runtime, nil
	}
	if m.resources == nil {
		m.resources = &resources{}
	}
	subPath := field
	if ref.StoreWithHash {
		subPath = zutils.GetHashEncode(field)
	}
	runtime := &fieldIndex{
		name:      path.Join(m.options.Name, field),
		storeName: path.Join(m.options.StoreName, subPath),
		settings:  cfg, resources: m.resources,
		segments: make(map[int]*IVFPQSegment),
	}
	if cfg.TargetType == vectorstore.HNSW {
		runtime.hnsw = newHNSWSegment(runtime.storeName, cfg.Dims, m.resources)
	}
	m.fields[field] = runtime
	return runtime, nil
}

// Close requires all owner Index users to have drained. Unpublish everything
// before cleanup; a partially closed runtime is never reused, even on failure.
func (m *VecIndexManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	fields := m.fields
	m.fields = nil
	m.resources = nil
	m.mu.Unlock()
	var errs []error
	for field, runtime := range fields {
		if err := runtime.Close(); err != nil {
			errs = append(errs, fmt.Errorf("field %s: %w", field, err))
		}
	}
	return errors.Join(errs...)
}
