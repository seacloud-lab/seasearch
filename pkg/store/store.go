package store

import (
	"container/list"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/core"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/store/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"
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

func Stop() {
	closer.SignalAndWait()
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
	if err := core.CheckIndexName(name); err != nil {
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
	if _, err := metadata.Index.Get(name); err == nil {
		return nil, fmt.Errorf("index [%s] already exists: %w", name, ErrIndexAlreadyExists)
	} else if !errors.Is(err, zincerrors.ErrKeyNotFound) {
		return nil, fmt.Errorf("read index %s metadata: %w", name, err)
	}

	index, err := createIndex(request)
	if err != nil {
		return nil, err
	}
	if err := writeIndex(index); err != nil {
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
// Close failures discard cached objects but leave all persistent data intact.
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
		if err := index.closeResources(); err != nil {
			return fmt.Errorf("close index %s: %w", name, err)
		}
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

	if err := vector.Delete(storeName, storageType); err != nil {
		return fmt.Errorf("delete index %s vector data: %w", name, err)
	}
	if err := text.Delete(storeName, storageType); err != nil {
		return fmt.Errorf("delete index %s text data: %w", name, err)
	}
	if err := metadata.Index.Delete(name); err != nil && !errors.Is(err, zincerrors.ErrKeyNotFound) {
		return fmt.Errorf("delete index %s metadata: %w", name, err)
	}
	return nil
}

// Locker coordinates operations on indexes by name.
type Locker struct {
	mutex sync.Mutex
	items map[string]*LockerItem
}

type LockerItem struct {
	lifecycle sync.RWMutex
	cacheload sync.Mutex
}

func newLocker() *Locker {
	return &Locker{
		items: make(map[string]*LockerItem),
	}
}

func (locker *Locker) Lifecycle(name string) *sync.RWMutex {
	locker.mutex.Lock()
	defer locker.mutex.Unlock()

	item, ok := locker.items[name]
	if !ok {
		item = new(LockerItem)
		locker.items[name] = item
	}

	return &item.lifecycle
}

func (locker *Locker) CacheLoad(name string) *sync.Mutex {
	locker.mutex.Lock()
	defer locker.mutex.Unlock()

	item, ok := locker.items[name]
	if !ok {
		item = new(LockerItem)
		locker.items[name] = item
	}

	return &item.cacheload
}

// Cache tracks owner references and idle entries.
type Cache struct {
	mutex sync.Mutex
	items map[string]*CacheItem
	evict *list.List
}

type CacheItem struct {
	elem *list.Element
	refs int

	// idle is closed when refs reaches zero and replaced on the next acquisition.
	idle chan struct{}

	rtime time.Time

	index *Index
}

func newCache() *Cache {
	return &Cache{
		items: make(map[string]*CacheItem),
		evict: list.New(),
	}
}

func (cache *Cache) Acquire(name string) (*Index, bool) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	item, ok := cache.items[name]
	if !ok {
		return nil, false
	}

	if item.elem != nil {
		cache.evict.Remove(item.elem)
		item.elem = nil
	}

	if item.refs == 0 {
		item.idle = make(chan struct{})
	}
	item.refs++

	return item.index, true
}

func (cache *Cache) Release(name string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	item, ok := cache.items[name]
	if !ok {
		return
	}

	item.refs--

	if item.refs == 0 {
		close(item.idle)
		item.rtime = time.Now()
		item.elem = cache.evict.PushBack(item)
	}
}

func (cache *Cache) Add(index *Index) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	item := new(CacheItem)
	item.refs = 1
	item.idle = make(chan struct{})
	item.index = index
	cache.items[index.name] = item
}

// waitIdle requires the caller to hold the lifecycle write lock. This excludes
// acquisitions and replacement while existing holders release their references.
func (cache *Cache) waitIdle(name string) *Index {
	cache.mutex.Lock()
	item, ok := cache.items[name]
	if !ok {
		cache.mutex.Unlock()
		return nil
	}
	idle := item.idle
	cache.mutex.Unlock()

	<-idle
	return item.index
}

func (cache *Cache) Del(name string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	item, ok := cache.items[name]
	if !ok {
		return
	}

	if item.elem != nil {
		cache.evict.Remove(item.elem)
		item.elem = nil
	}
	delete(cache.items, name)
}

func (cache *Cache) Clean() {
	defer closer.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case <-ticker.C:
			err := cache.clean()
			if err != nil {
				log.Warn().Err(err).Msg("failed to clean index cache")
			}
		}
	}
}

func (cache *Cache) clean() error {
	cutoff := time.Now().Add(time.Minute * -15)

	var names []string
	cache.mutex.Lock()
	for elem := cache.evict.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*CacheItem)
		if item.rtime.After(cutoff) {
			break
		}
		names = append(names, item.index.name)
	}
	cache.mutex.Unlock()

	var errs []error
	for _, name := range names {
		if err := cache.cleanIndex(name, cutoff); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (cache *Cache) cleanIndex(name string, cutoff time.Time) error {
	locker.Lifecycle(name).Lock()
	defer locker.Lifecycle(name).Unlock()

	cache.mutex.Lock()
	item, ok := cache.items[name]
	if !ok || item.refs != 0 || item.rtime.After(cutoff) {
		cache.mutex.Unlock()
		return nil
	}
	index := item.index
	cache.mutex.Unlock()

	// When unsaved metadata tracking is introduced, save it here before
	// unpublishing the instance; a save failure must leave it cached for retry.
	// The lifecycle write lock keeps this zero-reference entry stable and blocks
	// reloading until all resource close calls have finished.
	cache.Del(name)
	if err := index.closeResources(); err != nil {
		return fmt.Errorf("evict index %s: %w", name, err)
	}

	return nil
}
