package store

import "sync"

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
