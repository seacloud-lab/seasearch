package lru_cache

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/blevesearch/mmap-go"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"

	"github.com/zincsearch/zincsearch/pkg/config"
)

const pidFilename = "bluge.pid"
const TempExt = ".temp"

type LruCache struct {
	caches   map[string]*CacheFile
	readyMap map[string]chan struct{}
	maxSize  int64
	lock     sync.RWMutex
	closer   *z.Closer
	// check cache file Ext name
	fileExt extCheck
	// check temp file Ext name
	tempExt  string
	rootPath string
}

type extCheck func(ext string) bool

// Instance for bluge directory to avoid multiple cache instances
var Instance = GetCache(config.Global.DataPath, func(ext string) bool {
	return ext == index.ItemKindSnapshot || ext == index.ItemKindSegment
}, TempExt)

func GetCache(rootPath string, fileExt extCheck, tempExt string) *LruCache {
	cacheManager := &LruCache{
		caches:   make(map[string]*CacheFile),
		maxSize:  config.Global.ObjCache.MaxCacheSize,
		closer:   z.NewCloser(1),
		readyMap: make(map[string]chan struct{}),
		rootPath: rootPath,
		fileExt:  fileExt,
		tempExt:  tempExt,
	}
	err := cacheManager.init()
	if err != nil {
		panic(fmt.Sprintf("init cache error: %v", err))
	}

	go cacheManager.cleanup()
	return cacheManager
}

func (c *LruCache) Close() error {
	c.closer.SignalAndWait()
	return nil
}

func (c *LruCache) init() error {
	_, err := os.Stat(c.rootPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(c.rootPath, os.ModePerm)
		if err != nil {
			return err
		}
	}
	err = filepath.Walk(c.rootPath, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return err
		}

		ext := path.Ext(info.Name())
		// remove temp files
		if ext == c.tempExt {
			return os.Remove(p)
		}
		if !c.fileExt(ext) {
			return err
		}
		c.caches[p] = &CacheFile{
			path:     p,
			refCount: 0,
		}

		return err
	})
	return err
}

func (c *LruCache) cleanup() {
	defer c.closer.Done()

	tick := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-tick.C:
			err := c.doCleanup()
			if err != nil {
				log.Error().Err(err).Msgf("cleanup error: %s", err)
			}
		case <-c.closer.HasBeenClosed():
			return
		}
	}
}

type tempFile struct {
	aTime time.Time
	path  string
	size  int64
}

func (c *LruCache) doCleanup() error {
	var tempFiles []tempFile
	var curSize int64 = 0
	var targetSize = float64(c.maxSize) * 0.7

	err := filepath.Walk(c.rootPath, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return err
		}

		ext := path.Ext(info.Name())
		if !c.fileExt(ext) {
			return err
		}
		fi, err1 := os.Stat(p)
		if err1 != nil {
			return err1
		}

		curSize += fi.Size()

		statT := fi.Sys().(*syscall.Stat_t)

		tempFiles = append(tempFiles, tempFile{
			aTime: GetAtime(statT),
			size:  fi.Size(),
			path:  p,
		})

		return nil
	})

	if err != nil {
		return err
	}

	if len(tempFiles) == 0 {
		log.Debug().Msgf("clean up finished: there are no cached files")
		return nil
	}

	if float64(curSize) < targetSize {
		log.Debug().Msgf("clean up finished: cache size is below max")
		return nil
	}

	sort.Slice(tempFiles, func(i, j int) bool {
		return tempFiles[i].aTime.Before(tempFiles[j].aTime)
	})

	c.lock.Lock()
	defer c.lock.Unlock()

	for _, f := range tempFiles {
		if cf, ok := c.caches[f.path]; ok {
			if cf.refCount > 0 {
				continue
			}
		}
		delete(c.caches, f.path)
		err := os.Remove(f.path)
		if err != nil {
			return err
		}
		curSize -= f.size

		if float64(curSize) <= targetSize {
			break
		}
	}
	return nil
}

// CacheFile
// Prevent duplicate downloads from obj_store
// the filePath is the absolute path of cache file.
func (c *LruCache) CacheFile(filePath string, reader io.Reader) (*CacheFile, error) {
	dir, _ := filepath.Split(filePath)
	err := c.setup(dir, false)
	if err != nil {
		return nil, err
	}
	for {
		c.lock.Lock()
		// the file has already in local and cached.
		if cf, ok := c.caches[filePath]; ok {
			cf.refCount++
			c.lock.Unlock()
			return cf, nil
		}
		if ready, ok := c.readyMap[filePath]; ok {
			c.lock.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		c.readyMap[filePath] = ready
		c.lock.Unlock()

		tempfile, err := os.CreateTemp(c.rootPath, fmt.Sprintf("*%s", c.tempExt))
		cleanup := func() {
			c.lock.Lock()
			close(ready)
			delete(c.readyMap, filePath)
			c.lock.Unlock()
		}
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(tempfile.Name())
			cleanup()
			return nil, err
		}
		_, err = io.Copy(tempfile, reader)
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(tempfile.Name())
			cleanup()
			return nil, err
		}
		err = os.Rename(tempfile.Name(), filePath)
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(tempfile.Name())
			cleanup()
			return nil, err
		}
		err = tempfile.Sync()
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(filePath)
			cleanup()
			return nil, err
		}
		err = tempfile.Close()
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(filePath)
			cleanup()
			return nil, err
		}

		c.lock.Lock()
		close(ready) // wakes all goroutines waiting on the channel
		c.caches[filePath] = &CacheFile{
			path:     filePath,
			ref:      c,
			refCount: 1,
		}
		delete(c.readyMap, filePath)
		c.lock.Unlock()

		return c.caches[filePath], nil
	}
}

// UpdateCacheFile
// the filePath is the absolute path of cache file.
func (c *LruCache) UpdateCacheFile(inputFile string, filePath string) (*CacheFile, error) {
	dir, _ := filepath.Split(filePath)
	err := c.setup(dir, false)
	if err != nil {
		return nil, err
	}
	err = os.Rename(inputFile, filePath)
	if err != nil {
		return nil, err
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if cf, ok := c.caches[filePath]; ok {
		return cf, nil
	}
	c.caches[filePath] = &CacheFile{
		ref:      c,
		path:     filePath,
		refCount: 1,
	}
	return c.caches[filePath], nil
}

// GetCacheFile
// the filePath is the absolute path of cache file.
func (c *LruCache) GetCacheFile(filePath string) (*CacheFile, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	f, ok := c.caches[filePath]
	if !ok {
		return nil, false
	}
	f.refCount++

	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		delete(c.caches, filePath)
		return nil, false
	}
	return f, true
}

func (c *LruCache) setup(path string, readOnly bool) error {
	dirExists, err := dirExists(path)
	if err != nil {
		return fmt.Errorf("error checking if directory exists '%s': %w", path, err)
	}
	if !dirExists {
		if readOnly {
			return fmt.Errorf("readOnly, directory does not exist")
		}
		err = os.MkdirAll(path, 0777)
		if err != nil {
			return fmt.Errorf("error creating directory '%s': %w", path, err)
		}
	}
	return nil
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// Remove
// the filePath is the absolute path of cache file.
func (c *LruCache) Remove(filepath string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.caches, filepath)
	return os.Remove(filepath)
}

func (c *LruCache) OpenWriter(filePath string) (io.WriteCloser, error) {
	dir, _ := filepath.Split(filePath)
	err := c.setup(dir, false)
	if err != nil {
		return nil, err
	}
	c.lock.Lock()
	var cf *CacheFile
	var ok bool
	if cf, ok = c.caches[filePath]; ok {
		cf.refCount++
		c.lock.Unlock()
	} else {
		cf = &CacheFile{
			path:     filePath,
			refCount: 1,
			ref:      c,
		}
		c.caches[filePath] = cf
		c.lock.Unlock()
	}
	tempfile, err := os.CreateTemp(c.rootPath, fmt.Sprintf("*%s", c.tempExt))
	if err != nil {
		_ = tempfile.Close()
		return nil, err
	}
	wc := &tempWriteFile{
		tempPath: tempfile.Name(),
		cf:       cf,
		realPath: filePath,
		f:        tempfile,
	}
	return wc, nil
}

func (c *LruCache) Sync(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error opening directory for sync: %w", err)
	}
	err = dir.Sync()
	if err != nil {
		_ = dir.Close()
		return fmt.Errorf("error syncing directory: %w", err)
	}
	err = dir.Close()
	if err != nil {
		return fmt.Errorf("error closing directing after sync: %w", err)
	}
	return nil
}

func (c *LruCache) Lock(path string) (pid *os.File, err error) {
	pidPath := filepath.Join(path, pidFilename)
	pid, err = os.OpenFile(pidPath, os.O_CREATE|os.O_RDWR, 0777)
	err = unix.Flock(int(pid.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		_ = pid.Close()
		return
	}
	if err != nil {
		return pid, fmt.Errorf("unable to obtain exclusive access: %w", err)
	}
	err = pid.Truncate(0)
	if err != nil {
		return pid, fmt.Errorf("error truncating pid file: %w", err)
	}
	_, err = pid.Write([]byte(fmt.Sprintf("%d\n", os.Getpid())))
	if err != nil {
		return pid, fmt.Errorf("error writing pid: %w", err)
	}
	err = pid.Sync()
	if err != nil {
		return pid, fmt.Errorf("error syncing pid file: %w", err)
	}
	return
}

func (c *LruCache) Unlock(path string) error {
	pidPath := filepath.Join(path, pidFilename)
	err := os.RemoveAll(pidPath)
	if err != nil {
		return fmt.Errorf("error removing pid file: %w", err)
	}
	return nil
}

type tempWriteFile struct {
	f        *os.File
	cf       *CacheFile
	tempPath string
	realPath string
}

func (t *tempWriteFile) Write(p []byte) (n int, err error) {
	return t.f.Write(p)
}

func (t *tempWriteFile) Close() error {
	t.cf.Close()
	err := os.Rename(t.tempPath, t.realPath)
	if err != nil {
		return err
	}
	err = t.f.Sync()
	if err != nil {
		return err
	}
	return t.f.Close()
}

type CacheFile struct {
	ref      *LruCache
	path     string
	refCount uint
}

func (c *CacheFile) GetPath() string {
	return c.path
}

func (c *CacheFile) Close() error {
	if c == nil {
		return nil
	}
	if c.ref == nil {
		return nil
	}
	c.ref.lock.Lock()
	c.refCount--
	c.ref.lock.Unlock()
	return nil
}

type closerFunc func() error

func (c closerFunc) Close() error {
	return c()
}

func (c *CacheFile) getMmCloser(mm mmap.MMap, f *os.File) closerFunc {
	cf := func() error {
		err := mm.Unmap()
		// try to close file even if unmap failed
		err2 := f.Close()
		if err == nil {
			// try to return first error
			err = err2
		}
		_ = c.Close()
		return err
	}
	return cf
}

func (c *CacheFile) LoadReadOnlyData() (*segment.Data, io.Closer, error) {
	f, err := os.OpenFile(c.path, os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, err
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		// mmap failed, try to close the file
		_ = f.Close()
		return nil, nil, err
	}

	return segment.NewDataBytes(mm), c.getMmCloser(mm, f), nil
}
