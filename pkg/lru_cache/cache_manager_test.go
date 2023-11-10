package lru_cache

import (
	"bytes"
	"github.com/blugelabs/bluge/index"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

func TestLru(t *testing.T) {
	temp := t.TempDir()
	defer os.Remove(temp)
	m := LruCache{
		caches:   make(map[string]*CacheFile),
		readyMap: make(map[string]chan struct{}),
		maxSize:  1000,
		rootPath: temp,
		fileExt: func(ext string) bool {
			return ext == index.ItemKindSnapshot || ext == index.ItemKindSegment
		},
		tempExt: TempExt,
	}

	// this will be removed, it's the first access file
	cache1 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t1.seg"),
	}
	// this will be remained, cause refCount
	cache2 := &CacheFile{
		refCount: 1,
		path:     path.Join(temp, "t2.seg"),
	}

	// this will be removed
	cache3 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t3.seg"),
	}
	// this will be remained, it's under 70% max
	cache4 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t4.seg"),
	}

	m.caches[cache1.path] = cache1
	m.caches[cache2.path] = cache2
	m.caches[cache3.path] = cache3
	m.caches[cache4.path] = cache4

	var slice = []*CacheFile{cache1, cache2, cache3, cache4}

	for _, c := range slice {
		fi, _ := os.OpenFile(c.path, os.O_CREATE|os.O_RDWR, 0777)
		b := bytes.Buffer{}
		for i := 0; i < 250; i++ {
			b.Write([]byte("1"))
		}
		_, _ = fi.Write(b.Bytes())
		_ = fi.Sync()
		_ = fi.Close()
		time.Sleep(100 * time.Millisecond)
	}

	err := m.doCleanup()
	assert.Nil(t, err)

	_, ok := m.caches[cache1.path]
	assert.False(t, ok)
	_, ok = m.caches[cache2.path]
	assert.True(t, ok)

	_, ok = m.caches[cache3.path]
	assert.False(t, ok)

	_, ok = m.caches[cache4.path]
	assert.True(t, ok)
}

func TestCacheFile(t *testing.T) {
	temp := t.TempDir()
	defer os.Remove(temp)
	Manager := &LruCache{
		caches:   make(map[string]*CacheFile),
		maxSize:  1000,
		rootPath: temp,
		fileExt: func(ext string) bool {
			return ext == index.ItemKindSnapshot || ext == index.ItemKindSegment
		},
		tempExt: TempExt,
	}
	writer, err := Manager.OpenWriter(path.Join(temp, "t1.seg"))
	assert.Nil(t, err)

	content := "test-content-xxxxxxx\\of"
	_, err = writer.Write([]byte(content))
	assert.Nil(t, err)
	cf, ok := Manager.caches[path.Join(temp, "t1.seg")]
	assert.True(t, ok)
	assert.Equal(t, uint(1), cf.refCount)
	err = writer.Close()
	assert.Nil(t, err)
	assert.Equal(t, uint(0), cf.refCount)

	cf, exists := Manager.GetCacheFile(path.Join(temp, "t1.seg"))
	assert.True(t, exists)
	data, closer, err := cf.LoadReadOnlyData()
	assert.Nil(t, err)
	assert.Equal(t, uint(1), cf.refCount)

	buffer := &bytes.Buffer{}
	_, err = data.WriteTo(buffer)
	assert.Nil(t, err)

	assert.Equal(t, content, buffer.String())

	err = closer.Close()
	assert.Nil(t, err)

	assert.Equal(t, uint(0), cf.refCount)
}
