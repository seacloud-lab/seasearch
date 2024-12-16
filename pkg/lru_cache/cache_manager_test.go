package lru_cache

import (
	"bytes"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/blugelabs/bluge/index"
	"github.com/stretchr/testify/assert"
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
		size:     250,
		atime:    time.Now().Add(-4 * time.Second),
	}
	// this will be remained, cause refCount
	cache2 := &CacheFile{
		refCount: 1,
		path:     path.Join(temp, "t2.seg"),
		size:     250,
		atime:    time.Now().Add(-3 * time.Second),
	}

	// this will be removed
	cache3 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t3.seg"),
		size:     250,
		atime:    time.Now().Add(-2 * time.Second),
	}
	// this will be remained, it's under 70% max
	cache4 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t4.seg"),
		size:     250,
		atime:    time.Now().Add(-1 * time.Second),
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
	}

	err := m.makeRoomForCacheFile()
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

func TestInitLruCache(t *testing.T) {
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

	cache1 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t1.seg"),
		size:     250,
	}
	cache2 := &CacheFile{
		refCount: 1,
		path:     path.Join(temp, "t2.seg"),
		size:     250,
	}
	cache3 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t3.seg"),
		size:     250,
	}
	cache4 := &CacheFile{
		refCount: 0,
		path:     path.Join(temp, "t4.seg"),
		size:     250,
	}
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

	err := m.init()
	assert.Nil(t, err)
	totalSize := int64(0)
	for _, c := range m.caches {
		totalSize += c.size
	}

	assert.Equal(t, int64(1000), totalSize)
}

func TestCacheFileSize(t *testing.T) {
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

	b := bytes.Buffer{}
	for i := 0; i < 250; i++ {
		b.Write([]byte("1"))
	}

	storePath := path.Join(temp, "store.seg")
	cacheFile, err := m.CacheFile(storePath, &b)
	assert.Nil(t, err)
	assert.Equal(t, int64(250), cacheFile.size)
	s, ok := m.caches[storePath]
	assert.True(t, ok)
	assert.Equal(t, int64(250), s.size)
}

func TestOpenWriterSize(t *testing.T) {
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

	fpath := path.Join(temp, "store.seg")
	writer, err := m.OpenWriter(fpath)
	assert.Nil(t, err)

	b := bytes.Buffer{}
	for i := 0; i < 250; i++ {
		b.Write([]byte("1"))
	}

	n, err := io.Copy(writer, &b)
	assert.Nil(t, err)
	assert.Equal(t, int64(250), n)

	cf, ok := m.caches[fpath]
	assert.True(t, ok)
	assert.Equal(t, int64(0), cf.size)

	assert.Nil(t, writer.Close())

	cf, ok = m.caches[fpath]
	assert.True(t, ok)
	assert.Equal(t, int64(250), cf.size)
}

func TestUpdateCacheSize(t *testing.T) {
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
	fPath := path.Join(temp, "temp.seg")
	fi, _ := os.OpenFile(fPath, os.O_CREATE|os.O_RDWR, 0777)
	b := bytes.Buffer{}
	for i := 0; i < 250; i++ {
		b.Write([]byte("1"))
	}
	_, _ = fi.Write(b.Bytes())
	_ = fi.Sync()
	_ = fi.Close()
	realPath := path.Join(temp, "store.seg")
	assert.Nil(t, m.UpdateCacheFile(fPath, realPath))

	cf, ok := m.caches[realPath]
	assert.True(t, ok)
	assert.Equal(t, int64(250), cf.size)
}
