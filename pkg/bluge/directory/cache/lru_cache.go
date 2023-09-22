package cache

import (
	"context"
	"errors"
	"fmt"
	"github.com/blevesearch/mmap-go"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"golang.org/x/sys/unix"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

const pidFilename = "bluge.pid"
const tempExt = ".temp"

var (
	ErrFileNotCached = errors.New("file is not cached")
	Manager          *CacheManager
)

type CacheManager struct {
	caches   map[string]*cacheFile
	readyMap map[string]chan struct{}
	rootPath string
	maxSize  int64
	lock     sync.RWMutex
	closer   context.Context
	close    context.CancelFunc
	wg       sync.WaitGroup
}

type cacheFile struct {
	path     string
	refCount uint
}

type closerFunc func() error

func (c closerFunc) Close() error {
	return c()
}

func Init() {
	if config.Global.StorageType != "oss" && config.Global.StorageType != "s3" {
		return
	}
	ctx, closer := context.WithCancel(context.Background())
	Manager = &CacheManager{
		caches:   make(map[string]*cacheFile),
		maxSize:  config.Global.ObjCache.MaxCacheSize,
		rootPath: config.Global.DataPath,
		closer:   ctx,
		close:    closer,
		readyMap: make(map[string]chan struct{}),
	}
	err := Manager.InitCache()
	if err != nil {
		panic(fmt.Sprintf("init cache error: %v", err))
	}

	go Manager.cleanup()
}

func Close() {
	if config.Global.StorageType != "oss" && config.Global.StorageType != "s3" {
		return
	}
	if Manager == nil {
		return
	}

	Manager.close()
	Manager.wg.Wait()
}

func (c *CacheManager) InitCache() error {
	err := filepath.Walk(c.rootPath, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return err
		}

		ext := path.Ext(info.Name())
		// remove temp files
		if ext == tempExt {
			return os.Remove(p)
		}
		if ext != index.ItemKindSegment && ext != index.ItemKindSnapshot {
			return err
		}
		c.caches[p] = &cacheFile{
			path:     p,
			refCount: 0,
		}

		return err
	})
	return err
}

func (c *CacheManager) cleanup() {
	c.wg.Add(1)
	defer c.wg.Done()

	tick := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-tick.C:
			err := c.doCleanup()
			if err != nil {
				log.Error().Err(err).Msgf("cleanup error: %s", err)
			}
		case <-c.closer.Done():
			return
		}
	}
}

type tempFile struct {
	aTime time.Time
	path  string
	size  int64
}

func (c *CacheManager) doCleanup() error {
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
		if ext != index.ItemKindSegment && ext != index.ItemKindSnapshot {
			return err
		}
		fi, err1 := os.Stat(p)
		if err1 != nil {
			return err1
		}

		curSize += fi.Size()

		statT := fi.Sys().(*syscall.Stat_t)

		tempFiles = append(tempFiles, tempFile{
			aTime: getAtime(statT),
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

func (c *CacheManager) Load(filepath string) (*segment.Data, io.Closer, error) {
	c.lock.Lock()
	if f, ok := c.caches[filepath]; ok {
		f.refCount++
		c.lock.Unlock()
		return f.loadReadOnlyData()
	}
	return nil, nil, ErrFileNotCached
}

func (c *CacheManager) Setup(path string, readOnly bool) error {
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

// CacheFile
// Prevent duplicate downloads from obj_store
func (c *CacheManager) CacheFile(filePath string, reader io.Reader) error {
	for {
		c.lock.Lock()
		// the file has already in local and traced.
		if _, ok := c.caches[filePath]; ok {
			c.lock.Unlock()
			return nil
		}
		if ready, ok := c.readyMap[filePath]; ok {
			c.lock.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		c.readyMap[filePath] = ready
		c.lock.Unlock()

		tempfile, err := os.CreateTemp(c.rootPath, fmt.Sprintf("*%s", tempExt))
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
			return err
		}
		_, err = io.Copy(tempfile, reader)
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(tempfile.Name())
			cleanup()
			return err
		}
		err = os.Rename(tempfile.Name(), filePath)
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(tempfile.Name())
			cleanup()
			return err
		}
		err = tempfile.Sync()
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(filePath)
			cleanup()
			return err
		}
		err = tempfile.Close()
		if err != nil {
			_ = tempfile.Close()
			_ = c.Remove(filePath)
			cleanup()
			return err
		}

		c.lock.Lock()
		close(ready) // wakes all goroutines waiting on the channel
		c.caches[filePath] = &cacheFile{
			path:     filePath,
			refCount: 0,
		}
		delete(c.readyMap, filePath)
		c.lock.Unlock()

		return nil
	}
}

func (c *CacheManager) Remove(filepath string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.caches, filepath)
	return os.Remove(filepath)
}

type tempWriteFile struct {
	f        *os.File
	cf       *cacheFile
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

func (c *CacheManager) OpenWriter(filePath string) (io.WriteCloser, error) {
	c.lock.Lock()
	var cf *cacheFile
	var ok bool
	if cf, ok = c.caches[filePath]; ok {
		cf.refCount++
		c.lock.Unlock()
	} else {
		cf = &cacheFile{
			path:     filePath,
			refCount: 1,
		}
		c.caches[filePath] = cf
		c.lock.Unlock()
	}
	tempfile, err := os.CreateTemp(c.rootPath, fmt.Sprintf("*%s", tempExt))

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

func (c *CacheManager) Sync(path string) error {
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

func (c *CacheManager) Lock(path string) (pid *os.File, err error) {
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

func (c *CacheManager) Unlock(path string) error {
	pidPath := filepath.Join(path, pidFilename)
	err := os.RemoveAll(pidPath)
	if err != nil {
		return fmt.Errorf("error removing pid file: %w", err)
	}
	return nil
}

func (c *cacheFile) loadReadOnlyData() (*segment.Data, io.Closer, error) {
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

	return segment.NewDataBytes(mm), c.getCloser(mm, f), nil
}

func (c *cacheFile) getCloser(mm mmap.MMap, f *os.File) closerFunc {
	cf := func() error {
		err := mm.Unmap()
		// try to close file even if unmap failed
		err2 := f.Close()
		if err == nil {
			// try to return first error
			err = err2
		}
		c.Close()
		return err
	}
	return cf
}

func (c *cacheFile) Close() {
	Manager.lock.Lock()
	c.refCount--
	Manager.lock.Unlock()
}
