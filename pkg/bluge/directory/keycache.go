package directory

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/haiwen/goutils/objclient"
)

var (
	objectKeys *objectKeyCache = newObjectKeyCache()
)

// objectKeyCache caches object keys and "fully cached" prefixes.
type objectKeyCache struct {
	mutex sync.RWMutex

	// prefixes stores path prefixes that are known to be fully listed in the
	// cache.
	prefixes map[string]int64

	keys *btree.BTreeG[string]
}

func newObjectKeyCache() *objectKeyCache {
	var cache objectKeyCache
	cache.prefixes = make(map[string]int64)
	cache.keys = btree.NewOrderedG[string](32)
	return &cache
}

func (cache *objectKeyCache) Insert(key string) {
	cache.mutex.Lock()
	cache.keys.ReplaceOrInsert(key)
	cache.mutex.Unlock()
}

func (cache *objectKeyCache) Remove(path string) {
	cache.mutex.Lock()
	cache.keys.Clone().AscendGreaterOrEqual(path, func(key string) bool {
		if !strings.HasPrefix(key, path) {
			return false
		}
		cache.keys.Delete(key)
		return true
	})
	cache.mutex.Unlock()
}

func (cache *objectKeyCache) List(ctx context.Context, client objclient.Client, prefix string) ([]string, error) {
	cache.mutex.RLock()

	var keys []string
	if _, cached := cache.prefixes[prefix]; cached {
		cache.keys.AscendGreaterOrEqual(prefix, func(key string) bool {
			if !strings.HasPrefix(key, prefix) {
				return false
			}
			keys = append(keys, key)
			return true
		})
		cache.mutex.RUnlock()
		return keys, nil
	}
	cache.mutex.RUnlock()

	items, err := client.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		keys = append(keys, item.Key)
	}

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	// Remove keys under `prefix` from the main keys b-tree (they will be
	// replaced by the freshly fetched keys below).
	cache.keys.Clone().AscendGreaterOrEqual(prefix, func(key string) bool {
		if !strings.HasPrefix(key, prefix) {
			return false
		}
		cache.keys.Delete(key)
		return true
	})
	for _, key := range keys {
		cache.keys.ReplaceOrInsert(key)
	}

	cache.prefixes[prefix] = time.Now().Unix()

	return keys, nil
}
