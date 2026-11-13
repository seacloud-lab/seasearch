package store

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zincsearch/zincsearch/pkg/cluster"

	"github.com/rs/zerolog/log"
)

// Cache tracks owner references and idle entries.
type Cache struct {
	mutex sync.Mutex
	items map[string]*CacheItem
	evict *list.List

	assignChanged <-chan struct{}
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
		items:         make(map[string]*CacheItem),
		evict:         list.New(),
		assignChanged: cluster.AssignmentChanged,
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
// A timeout leaves the cache entry and its resources unchanged.
func (cache *Cache) waitIdle(name string, timeout time.Duration) (*Index, error) {
	cache.mutex.Lock()
	item, ok := cache.items[name]
	if !ok {
		cache.mutex.Unlock()
		return nil, nil
	}
	idle := item.idle
	cache.mutex.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-idle:
		return item.index, nil
	case <-timer.C:
		return nil, ErrIndexDrainTimeout
	}
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
		case <-cache.assignChanged:
			err := cache.cleanUnassigned()
			if err != nil {
				log.Warn().Err(err).Msg("failed to clean unassigned indexes")
			}
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
		err := cache.cleanIndex(name, cutoff)
		if err != nil {
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

	// A save failure leaves the latest metadata and runtime cached for retry.
	err := writeIndex(index)
	if err != nil {
		return fmt.Errorf("evict index %s: %w", name, err)
	}
	// The lifecycle write lock keeps this zero-reference entry stable and blocks
	// reloading until all resource close calls have finished.
	cache.Del(name)
	index.closeResources()

	return nil
}

// close is shutdown cleanup, not eviction. Callers must already be drained and
// the scanner stopped. Save failures must not prevent any resource close calls.
func (cache *Cache) close() error {
	cache.mutex.Lock()
	names := make([]string, 0, len(cache.items))
	for name := range cache.items {
		names = append(names, name)
	}
	cache.mutex.Unlock()

	var errs []error
	for _, name := range names {
		err := cache.closeIndex(name)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (cache *Cache) closeIndex(name string) error {
	lifecycle := locker.Lifecycle(name)
	lifecycle.Lock()
	defer lifecycle.Unlock()

	cache.mutex.Lock()
	item, ok := cache.items[name]
	cache.mutex.Unlock()
	if !ok {
		return nil
	}

	index := item.index
	err := writeIndex(index)
	cache.Del(name)
	index.closeResources()
	if err != nil {
		return fmt.Errorf("stop index %s: %w", name, err)
	}
	return nil
}
