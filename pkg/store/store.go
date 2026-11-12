package store

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/core"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/store/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
)

var (
	closer = z.NewCloser(0)
	locker = newLocker()
	cache  = newCache()

	deleteIndexNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
)

func Start() error {
	closer.AddRunning(1)
	go cache.Clean()
	return nil
}

// Stop requires external callers and background tasks to have released their
// Index references. It stops and drains store vector maintenance, stops cache
// scanning, then saves and closes every cached instance.
func Stop() {
	StopVectorMaintenance()
	closer.SignalAndWait()
	err := cache.close()
	if err != nil {
		log.Error().Err(err).Msg("failed to stop index store")
	}
}

func Open(name string) (*Index, error) {
	locker.Lifecycle(name).RLock()
	defer locker.Lifecycle(name).RUnlock()

	if !cluster.AssignCheck(name) {
		return nil, zincerrors.ErrIndexServerMismatch
	}

	index, ok := cache.Acquire(name)
	if ok {
		return index, nil
	}

	locker.CacheLoad(name).Lock()
	defer locker.CacheLoad(name).Unlock()

	index, ok = cache.Acquire(name)
	if ok {
		return index, nil
	}

	index, err := readIndex(name, false)
	if err != nil {
		return nil, err
	}
	cache.Add(index)

	return index, nil
}

func OpenOrCreate(name string) (*Index, bool, error) {
	locker.Lifecycle(name).RLock()
	defer locker.Lifecycle(name).RUnlock()

	if !cluster.AssignCheck(name) {
		return nil, false, zincerrors.ErrIndexServerMismatch
	}

	index, ok := cache.Acquire(name)
	if ok {
		return index, true, nil
	}

	locker.CacheLoad(name).Lock()
	defer locker.CacheLoad(name).Unlock()

	index, ok = cache.Acquire(name)
	if ok {
		return index, true, nil
	}

	index, err := readIndex(name, false)
	if errors.Is(err, ErrIndexNotFound) {
		// continue
	} else if err != nil {
		return nil, false, err
	} else {
		cache.Add(index)
		return index, true, nil
	}

	index, err = createIndex(&meta.IndexSimple{Name: name})
	if err != nil {
		return nil, false, err
	}
	err = writeIndex(index)
	if err != nil {
		// Construction opens no storage resources. Discard the unpublished
		// instance; Close would incorrectly release a cache reference. A failed
		// Set may have committed, so leave metadata for the next read to resolve.
		return nil, false, err
	}
	cache.Add(index)

	return index, false, nil
}

// Create strictly creates an index with its final configuration before publishing
// it. A successful result owns one reference which the caller must Close.
func Create(request *meta.IndexSimple) (*Index, error) {
	if request == nil {
		return nil, fmt.Errorf("index request must not be nil")
	}
	name := request.Name
	err := core.CheckIndexName(name)
	if err != nil {
		return nil, err
	}
	lifecycle := locker.Lifecycle(name)
	lifecycle.RLock()
	defer lifecycle.RUnlock()
	if !cluster.AssignCheck(name) {
		return nil, zincerrors.ErrIndexServerMismatch
	}
	locker.CacheLoad(name).Lock()
	defer locker.CacheLoad(name).Unlock()

	// Check existence without acquiring a reference or constructing a runtime
	// object. The lifecycle and cacheload locks exclude same-name publication.
	cache.mutex.Lock()
	_, cached := cache.items[name]
	cache.mutex.Unlock()
	if cached {
		return nil, fmt.Errorf("index [%s] already exists: %w", name, ErrIndexAlreadyExists)
	}
	_, err = metadata.Index.Get(name)
	if err == nil {
		return nil, fmt.Errorf("index [%s] already exists: %w", name, ErrIndexAlreadyExists)
	} else if !errors.Is(err, zincerrors.ErrKeyNotFound) {
		return nil, fmt.Errorf("read index %s metadata: %w", name, err)
	}

	index, err := createIndex(request)
	if err != nil {
		return nil, err
	}
	err = writeIndex(index)
	if err != nil {
		// No storage resources have been opened. A failed Set may have committed;
		// leave persistent metadata for the next read rather than rolling it back.
		return nil, err
	}
	cache.Add(index)
	return index, nil
}

// GetQueryIndex reuses the owner cache or returns an uncached read-only view.
// Read-only views do not participate in lifecycle draining; concurrent deletion
// or assignment changes may cause reads to fail. The caller must Close the
// result after closing all request Searchers and Readers.
func GetQueryIndex(name string) (*Index, error) {
	if cluster.AssignCheck(name) {
		index, err := Open(name)
		if err != nil {
			return nil, fmt.Errorf("failed to open: %w", err)
		}
		return index, nil
	}

	index, err := readIndex(name, true)
	if err != nil {
		return nil, fmt.Errorf("failed to read index: %w", err)
	}
	return index, nil
}

// Delete waits for owner references to drain before closing resources and
// removing data. Callers must release their own references before calling it.
// Resource cleanup is best-effort and does not prevent persistent data deletion.
func Delete(name string) error {
	if name == "" || name == "." || name == ".." || strings.HasPrefix(name, "_") || !deleteIndexNameRe.MatchString(name) {
		return fmt.Errorf("invalid index name %q", name)
	}

	locker.Lifecycle(name).Lock()
	defer locker.Lifecycle(name).Unlock()

	if !cluster.AssignCheck(name) {
		return zincerrors.ErrIndexServerMismatch
	}

	index := cache.waitIdle(name)
	var storeName, storageType string
	if index != nil {
		// No holder can access metadata now, and the lifecycle write lock keeps
		// the cache entry stable. Use the latest, possibly unsaved, metadata.
		storeName = index.GetStoreName()
		storageType = index.GetStorageType()
		// Unpublish even if resource cleanup fails. A partially closed object
		// must never be handed to a subsequent acquisition.
		cache.Del(name)
		index.closeResources()
	} else {
		// Cold deletion needs no analyzers, runtime shards or Writers.
		ref, err := metadata.Index.Get(name)
		if errors.Is(err, zincerrors.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read index %s metadata for deletion: %w", name, err)
		}
		if ref == nil || ref.Name != name {
			return fmt.Errorf("index %s metadata has an invalid name", name)
		}
		storeName = name
		if ref.StoreWithHash {
			hash := zutils.GetHashEncode(name)
			storeName = path.Join(hash[:2], hash[2:])
		}
		storageType = ref.StorageType
	}

	err := vector.Delete(storeName, storageType)
	if err != nil {
		return fmt.Errorf("delete index %s vector data: %w", name, err)
	}
	err = text.Delete(storeName, storageType)
	if err != nil {
		return fmt.Errorf("delete index %s text data: %w", name, err)
	}
	err = metadata.Index.Delete(name)
	if err != nil && !errors.Is(err, zincerrors.ErrKeyNotFound) {
		return fmt.Errorf("delete index %s metadata: %w", name, err)
	}
	return nil
}
