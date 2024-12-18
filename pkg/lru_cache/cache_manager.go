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
	"github.com/docker/go-units"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"

	"github.com/zincsearch/zincsearch/pkg/config"
)

const pidFilename = "bluge.pid"
const TempExt = ".temp"
const VectorIndexExt = ".index"

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

// Instance lru cache is globally unique, directory and vector_manager share the instance
var Instance *LruCache

func Init() {
	if config.Global.StorageType == "disk" {
		return
	}
	Instance = createCache(config.Global.DataPath, func(ext string) bool {
		return ext == index.ItemKindSnapshot || ext == index.ItemKindSegment || ext == VectorIndexExt
	}, TempExt)
}

func ShutDown() {
	if config.Global.StorageType == "disk" {
		return
	}
	_ = Instance.Close()
}

func createCache(rootPath string, fileExt extCheck, tempExt string) *LruCache {
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
		fi, err1 := os.Stat(p)
		if err1 != nil {
			return err1
		}
		statT := fi.Sys().(*syscall.Stat_t)
		c.caches[p] = &CacheFile{
			path:     p,
			refCount: 0,
			ref:      c,
			size:     fi.Size(),
			atime:    GetAtime(statT),
		}

		return err
	})
	return err
}

func (c *LruCache) makeRoomForCacheFile() error {
	var curSize int64 = 0
	c.lock.RLock()
	for _, f := range c.caches {
		curSize += f.size
	}
	if curSize < c.maxSize {
		c.lock.RUnlock()
		return nil
	}
	var tempFiles []*CacheFile
	for _, f := range c.caches {
		tempFiles = append(tempFiles, f)
	}
	c.lock.RUnlock()

	var targetSize = float64(c.maxSize) * 0.7

	if len(tempFiles) == 0 {
		log.Info().Msgf("clean up finished: there are no cached files")
		return nil
	}

	sort.Slice(tempFiles, func(i, j int) bool {
		return tempFiles[i].atime.Before(tempFiles[j].atime)
	})

	c.lock.Lock()
	defer c.lock.Unlock()
	var (
		oldTotalCacheSize = curSize
		oldCacheCount     = len(tempFiles)
		referedCount      = 0
		deleteCount       = int64(0)
		deletedSize       = int64(0)
	)

	for _, f := range tempFiles {
		if cf, ok := c.caches[f.path]; ok {
			if cf.refCount > 0 {
				referedCount++
				continue
			}
		}
		deleteCount++
		delete(c.caches, f.path)
		err := os.Remove(f.path)
		if err != nil {
			log.Warn().Err(err).Msgf("clean up local cache file err: ")
			continue
		}
		curSize -= f.size
		deletedSize += f.size
		if float64(curSize) <= targetSize {
			break
		}
	}

	log.Info().Msgf("LRU clean finished: old files: %d, old cache size: %s, refered files: %d, deleted files: %d, made room: %s",
		oldCacheCount, units.BytesSize(float64(oldTotalCacheSize)), referedCount, deleteCount, units.BytesSize(float64(deletedSize)))
	return nil
}

// CacheFile
// Prevent duplicate downloads from obj_store
// the filePath is the absolute path of cache file.
func (c *LruCache) CacheFile(filePath string, reader io.Reader) (*CacheFile, error) {
	dir, _ := filepath.Split(filePath)
	err := c.Setup(dir, false)
	if err != nil {
		return nil, err
	}
	err = c.makeRoomForCacheFile()
	if err != nil {
		return nil, fmt.Errorf("lru clean cache err: %w", err)
	}
	for {
		c.lock.Lock()
		// the file has already in local and cached.
		if cf, ok := c.caches[filePath]; ok {
			cf.refCount++
			cf.atime = time.Now()
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
			_ = tempfile.Close()
			_ = c.Remove(tempfile.Name())
			c.lock.Lock()
			close(ready)
			delete(c.readyMap, filePath)
			c.lock.Unlock()
		}
		if err != nil {
			log.Error().Err(err).Msgf("Cache %s err: create temp cache file error", filePath)
			cleanup()
			return nil, fmt.Errorf("cache file err: create temp file err: %w", err)
		}
		size, err := io.Copy(tempfile, reader)
		if err != nil {
			log.Error().Err(err).Msgf("Cache %s err: copy temp cache file error", filePath)
			cleanup()
			return nil, fmt.Errorf("cache file err: copy temp file err: %w", err)
		}
		err = os.Rename(tempfile.Name(), filePath)
		if err != nil {
			log.Error().Err(err).Msgf("Cache %s err: rename temp cache file error", filePath)
			cleanup()
			return nil, fmt.Errorf("cache file err: rename temp file err: %w", err)
		}
		err = tempfile.Sync()
		if err != nil {
			log.Error().Err(err).Msgf("Cache %s err: sync temp cache file error", filePath)
			cleanup()
			return nil, fmt.Errorf("cache file err: sync temp file err: %w", err)
		}
		err = tempfile.Close()
		if err != nil {
			log.Error().Err(err).Msgf("Cache %s err: close temp cache file error", filePath)
			cleanup()
			return nil, fmt.Errorf("cache file err: close temp file err: %w", err)
		}

		c.lock.Lock()
		close(ready) // wakes all goroutines waiting on the channel
		c.caches[filePath] = &CacheFile{
			path:     filePath,
			ref:      c,
			refCount: 1,
			size:     size,
			atime:    time.Now(),
		}
		delete(c.readyMap, filePath)
		c.lock.Unlock()

		return c.caches[filePath], nil
	}
}

// UpdateCacheFile
// the filePath is the absolute path of cache file.
func (c *LruCache) UpdateCacheFile(inputFile string, filePath string) error {
	dir, _ := filepath.Split(filePath)
	err := c.Setup(dir, false)
	if err != nil {
		return err
	}
	err = c.makeRoomForCacheFile()
	if err != nil {
		return fmt.Errorf("lru clean cache err: %w", err)
	}
	err = os.Rename(inputFile, filePath)
	if err != nil {
		log.Error().Err(err).Msgf("Rename file %s to %s err: ", inputFile, filePath)
		return fmt.Errorf("update cache file err: rename temp file err : %w", err)
	}
	stat, err := os.Stat(filePath)
	if err != nil {
		log.Error().Err(err).Msgf("Stat file %s err: ", filePath)
		return fmt.Errorf("update cache file err: stat file err : %w", err)
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.caches[filePath] = &CacheFile{
		ref:      c,
		path:     filePath,
		refCount: 0,
		size:     stat.Size(),
		atime:    time.Now(),
	}
	return nil
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
	f.atime = time.Now()
	_, err := os.Stat(filePath)
	if err != nil {
		delete(c.caches, filePath)
		return nil, false
	}
	return f, true
}

func (c *LruCache) Setup(path string, readOnly bool) error {
	dirExists, err := dirExists(path)
	if err != nil {
		log.Error().Err(err).Msgf("Setup %s err: check dir exists err: ", path)
		return fmt.Errorf("setup err: error checking if directory exists '%s': %w", path, err)
	}
	if !dirExists {
		err = os.MkdirAll(path, 0777)
		if err != nil {
			log.Error().Err(err).Msgf("Setup %s err: create dir err: ", path)
			return fmt.Errorf("setup err: error creating directory '%s': %w", path, err)
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
// The filePath is the absolute path of cache file.
// After remove the specified file, it also removes the file's parent folder recursive if it is empty
// until the parent is cache root.
func (c *LruCache) Remove(filepath string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.caches, filepath)
	err := os.RemoveAll(filepath)
	if err != nil {
		log.Error().Err(err).Msgf("Remove %s err: ", filepath)
		return fmt.Errorf("remove file %s err: %w", filepath, err)
	}

	// if parent dir is empty, remove parent folder
	err = c.removeParentIfEmpty(filepath)
	if err != nil {
		log.Error().Err(err).Msgf("Remove %s err: ", filepath)
	}
	return err
}

func (c *LruCache) removeParentIfEmpty(p string) error {
	if p == c.rootPath {
		return nil
	}
	dirPath := filepath.Dir(p)
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("remove parent err: read parent dir err: %w", err)
	}
	if len(entries) != 0 {
		return nil
	}
	err = os.RemoveAll(dirPath)
	if err != nil {
		return fmt.Errorf("remove parent dir err: %w", err)
	}
	return c.removeParentIfEmpty(dirPath)
}

func (c *LruCache) OpenWriter(filePath string) (io.WriteCloser, error) {
	dir, _ := filepath.Split(filePath)
	err := c.Setup(dir, false)
	if err != nil {
		return nil, fmt.Errorf("open cache writer err: %w", err)
	}
	c.lock.Lock()
	var cf *CacheFile
	var ok bool
	if cf, ok = c.caches[filePath]; ok {
		cf.refCount++
		cf.atime = time.Now()
		c.lock.Unlock()
	} else {
		cf = &CacheFile{
			path:     filePath,
			refCount: 1,
			ref:      c,
			atime:    time.Now(),
		}
		c.caches[filePath] = cf
		c.lock.Unlock()
	}
	tempfile, err := os.CreateTemp(c.rootPath, fmt.Sprintf("*%s", c.tempExt))
	if err != nil {
		log.Error().Err(err).Msgf("Open writer %s err: create temp file err", filePath)
		_ = tempfile.Close()
		return nil, fmt.Errorf("open cache writer err: crate temp file err: %w", err)
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
		log.Error().Err(err).Msgf("Sync err: open dir %s err:", path)
		return fmt.Errorf("error opening directory for sync: %w", err)
	}
	err = dir.Sync()
	if err != nil {
		_ = dir.Close()
		log.Error().Err(err).Msgf("Sync err: sync dir %s err:", path)
		return fmt.Errorf("error syncing directory: %w", err)
	}
	err = dir.Close()
	if err != nil {
		log.Error().Err(err).Msgf("Sync err: close dir %s err:", path)
		return fmt.Errorf("error closing directing after sync: %w", err)
	}
	return nil
}

func (c *LruCache) Lock(path string) (pid *os.File, err error) {
	pidPath := filepath.Join(path, pidFilename)
	pid, err = os.OpenFile(pidPath, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		_ = pid.Close()
		return
	}
	err = unix.Flock(int(pid.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		_ = pid.Close()
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
	_ = t.cf.Close()

	// delete old cache
	t.cf.ref.lock.Lock()
	delete(t.cf.ref.caches, t.realPath)
	t.cf.ref.lock.Unlock()

	defer func() {
		// we clear temp file
		_ = os.RemoveAll(t.tempPath)
	}()
	err := os.Rename(t.tempPath, t.realPath)
	if err != nil {
		log.Error().Err(err).Msgf("close temp write file %s err: rename err: ", t.realPath)
		return fmt.Errorf("close temp write file err: rename err: %w", err)
	}
	err = t.f.Sync()
	if err != nil {
		log.Error().Err(err).Msgf("close temp write file %s err: sync err: ", t.realPath)
		return fmt.Errorf("close temp write file err: sync err: %w", err)
	}
	stat, err := t.f.Stat()
	if err != nil {
		log.Error().Err(err).Msgf("close temp write file %s err: sync err: ", t.realPath)
		return fmt.Errorf("close temp write file err: stat file err: %w", err)
	}
	t.cf.ref.lock.Lock()
	t.cf.ref.caches[t.realPath] = &CacheFile{
		ref:      t.cf.ref,
		path:     t.realPath,
		refCount: 0,
		size:     stat.Size(),
		atime:    time.Now(),
	}
	t.cf.ref.lock.Unlock()

	return t.f.Close()
}

type CacheFile struct {
	ref      *LruCache
	path     string
	refCount uint
	size     int64
	atime    time.Time
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
		log.Error().Err(err).Msgf("Load readonly data %s err: open file err: ", c.path)
		return nil, nil, fmt.Errorf("load readonly data err: open file err: %w", err)
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB)
	if err != nil {
		log.Error().Err(err).Msgf("load readonly data %s err: Flock err: ", c.path)
		_ = f.Close()
		return nil, nil, fmt.Errorf("load readonly data err: flock err: %w", err)
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		log.Error().Err(err).Msgf("load readonly data %s err: MMap err: ", c.path)
		// mmap failed, try to close the file
		_ = f.Close()
		return nil, nil, fmt.Errorf("load readonly data err: mmap err: %w", err)
	}

	return segment.NewDataBytes(mm), c.getMmCloser(mm, f), nil
}
