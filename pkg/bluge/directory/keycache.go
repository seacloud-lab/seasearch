package directory

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/haiwen/goutils/objclient"
)

var (
	defaultKeyCache = newObjectKeyCache()
)

// objectKeyCache reduces the calls of objclient.List() by caching the keys of
// objects in memory.
type objectKeyCache struct {
	mutex sync.Mutex
	items map[string]*objectKeyCacheItem
}

type objectKeyCacheItem struct {
	// ready will be true once the keys are ready to be used.
	ready bool
	// invalid indicates that an invalidation request has been made while the
	// keys are being fetched. In this case, the keys will not be cached.
	invalid bool
	// keys is the cached keys of objects with the given prefix.
	keys []string
}

func newObjectKeyCache() *objectKeyCache {
	var cache objectKeyCache
	cache.items = make(map[string]*objectKeyCacheItem)
	return &cache
}

func (cache *objectKeyCache) List(ctx context.Context, client objclient.Client, prefix string) ([]string, error) {
	cache.mutex.Lock()
	item, ok := cache.items[prefix]
	if ok && item.ready {
		cache.mutex.Unlock()
		return item.keys, nil
	}
	item = new(objectKeyCacheItem)
	cache.items[prefix] = item
	cache.mutex.Unlock()

	objs, err := client.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(objs))
	for i, obj := range objs {
		keys[i] = obj.Key
	}

	cache.mutex.Lock()
	// If another List() was called, the item will be replaced.
	if cache.items[prefix] == item && !item.invalid {
		item.ready = true
		item.keys = keys
	}
	cache.mutex.Unlock()
	return keys, nil
}

func (cache *objectKeyCache) Invalidate(keys ...string) {
	cache.mutex.Lock()
	for prefix, item := range cache.items {
		var match bool
		for _, key := range keys {
			if strings.HasPrefix(key, prefix) {
				match = true
				break
			}
		}
		if !match {
			continue
		}

		if item.ready {
			delete(cache.items, prefix)
		} else {
			item.invalid = true
		}
	}
	cache.mutex.Unlock()
}

// KeyCacheClient wraps an objclient.Client and invalidates the cached keys
// when the objects are changed.
type KeyCacheClient struct {
	objclient.Client
}

func NewKeyCacheClient(backend objclient.Client) *KeyCacheClient {
	var client KeyCacheClient
	client.Client = backend
	return &client
}

func (client *KeyCacheClient) Write(ctx context.Context, key string, r io.Reader, o *objclient.WriteOptions) error {
	err := client.Client.Write(ctx, key, r, o)
	defaultKeyCache.Invalidate(key)
	return err
}

func (client *KeyCacheClient) Remove(ctx context.Context, keys ...string) error {
	err := client.Client.Remove(ctx, keys...)
	defaultKeyCache.Invalidate(keys...)
	return err
}

func (client *KeyCacheClient) Copy(ctx context.Context, src, dst string) error {
	err := client.Client.Copy(ctx, src, dst)
	defaultKeyCache.Invalidate(dst)
	return err
}
