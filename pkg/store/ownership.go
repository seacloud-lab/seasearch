package store

import (
	"errors"
	"fmt"

	"github.com/zincsearch/zincsearch/pkg/cluster"
)

// cleanUnassigned handles an ownership-change hint, not a historical assignment
// snapshot. Locker names include in-flight loads that have not published a cache
// entry yet. Only names escape the short-lived locks; each entry is rechecked
// under its lifecycle write lock. Failures are not queued for retry.
func (cache *Cache) cleanUnassigned() error {
	locker.mutex.Lock()
	names := make([]string, 0, len(locker.items))
	for name := range locker.items {
		names = append(names, name)
	}
	locker.mutex.Unlock()

	var errs []error
	for _, name := range names {
		select {
		case <-closer.HasBeenClosed():
			return errors.Join(errs...)
		default:
		}
		if cluster.AssignCheck(name) {
			continue
		}
		if err := cache.cleanUnassignedIndex(name); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (cache *Cache) cleanUnassignedIndex(name string) error {
	lifecycle := locker.Lifecycle(name)
	lifecycle.Lock()
	defer lifecycle.Unlock()

	select {
	case <-closer.HasBeenClosed():
		return nil
	default:
	}
	if cluster.AssignCheck(name) {
		return nil
	}

	cache.mutex.Lock()
	item, ok := cache.items[name]
	if !ok {
		cache.mutex.Unlock()
		return nil
	}
	idle := item.idle
	cache.mutex.Unlock()

	// The lifecycle write lock keeps this entry stable and excludes new
	// acquisitions. Release needs neither this lock nor the metadata lock.
	select {
	case <-closer.HasBeenClosed():
		return nil
	case <-idle:
	}
	select {
	case <-closer.HasBeenClosed():
		return nil
	default:
	}
	if cluster.AssignCheck(name) {
		return nil
	}
	index := item.index
	// Retain the only current metadata on save failure. A later assignment hint,
	// ordinary TTL eviction or Stop may try again; there is no periodic retry.
	if err := writeIndex(index); err != nil {
		return fmt.Errorf("unassign index %s: %w", name, err)
	}
	cache.Del(name)
	if err := index.closeResources(); err != nil {
		return fmt.Errorf("unassign index %s: %w", name, err)
	}
	return nil
}
