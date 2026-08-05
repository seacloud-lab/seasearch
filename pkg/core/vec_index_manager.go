package core

import (
	"cmp"
	"container/list"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/btree"
)

const (
	cacheTTL = 30 * time.Minute
)

var (
	ErrVecIndexNotExists  = errors.New("index not found")
	ErrVecIndexCorruption = errors.New("index file is corrupted")
	ErrInvalidVec         = errors.New("invalid vectors")
	ErrInvalidArguments   = errors.New("invalid arguments")

	vecIdxManager *VecIndexManager
)

type VecIndexManager struct {
	locker VectorIndexLocker
	cache  VectorIndexCache

	storage vector.ObjStore
	tmpDir  string // path for saving temp file

	closer *z.Closer

	sealTaskMp map[string]struct{}
	sealedLock sync.RWMutex
	sealCh     chan *sealTask

	// buildHNSWIndexTasks uses a thread-safe BTree to store queued and running
	// tasks. It ensures that each task is unique and prevents duplicate tasks
	// from being queued.
	buildHNSWIndexTasks *btree.BTreeG[BuildHNSWIndexTask]
	buildHNSWIndexChan  chan BuildHNSWIndexTask
}

func InitVecIndexManager() {
	storage, err := vector.GetVectorStorage()
	if err != nil {
		log.Fatal().Err(err).Msgf("get vector storage error: %s", err)
	}
	tmpDir := path.Join(config.Global.DataPath, vector.VecPrefix, "tmp")
	if _, err := os.Stat(tmpDir); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(tmpDir, os.ModePerm)
			if err != nil {
				log.Fatal().Err(err).Msgf("init vector manager error")
			}
		} else {
			log.Fatal().Err(err).Msgf("init vector manager error")
		}
	}

	vecIdxManager = new(VecIndexManager)
	vecIdxManager.locker.locks = make(map[string]*sync.Mutex)
	vecIdxManager.cache.items = make(map[string]*VectorIndexCacheItem)
	vecIdxManager.cache.evict = list.New()
	vecIdxManager.storage = storage
	vecIdxManager.tmpDir = tmpDir
	vecIdxManager.closer = z.NewCloser(4)
	vecIdxManager.sealTaskMp = make(map[string]struct{})
	vecIdxManager.sealCh = make(chan *sealTask, 10)
	vecIdxManager.buildHNSWIndexTasks = btree.NewBTreeG(BuildHNSWIndexTask.less)
	vecIdxManager.buildHNSWIndexChan = make(chan BuildHNSWIndexTask)

	go cacheCleaner()

	go backgroundSealCheck()

	go backgroundSeal()

	go backgroundBuildHNSWIndex()
}

func CloseVecIndexManager() {
	vecIdxManager.closer.SignalAndWait()
}

func OpenVectorIndex(indexName string, fieldName string) (VectorIndex, error) {
	key := path.Join(indexName, fieldName)

	vecIdxManager.locker.Lock(key)
	defer vecIdxManager.locker.Unlock(key)

	index, ok := vecIdxManager.cache.Acquire(key)
	if ok {
		return index, nil
	}

	index, err := openVectorIndex(fieldName, indexName)
	if err != nil {
		return nil, err
	}
	vecIdxManager.cache.Set(key, index)

	return index, nil
}

func openVectorIndex(fieldName, indexName string) (VectorIndex, error) {
	zincIndex, ok := GetIndex(indexName)
	if !ok {
		return nil, fmt.Errorf("try get zinc index %s for getting vector field %s, but zinc index not exists", indexName, fieldName)
	}

	vecIndexMeta, ok := zincIndex.GetVecIndex(fieldName)
	if !ok {
		// the vector index metadata should always exist unless the field not exists, or it's not a vector field.
		return nil, ErrVecIndexNotExists
	}

	return MakeVecIndex(zincIndex, fieldName, vecIndexMeta)
}

func CloseVectorIndex(index VectorIndex) {
	key := index.Name()

	vecIdxManager.locker.Lock(key)
	defer vecIdxManager.locker.Unlock(key)

	vecIdxManager.cache.Release(key)
}

func DeleteVectorIndex(indexName string, fieldName string) error {
	zincIndex, ok := GetIndex(indexName)
	if !ok {
		return fmt.Errorf("try get zinc index %s for getting vector field %s, but zinc index not exists", indexName, fieldName)
	}
	vecIndexMeta, ok := zincIndex.GetVecIndex(fieldName)
	if !ok {
		return nil
	}
	storeName := makeVecIndexStoreName(zincIndex.GetStoreName(), fieldName, vecIndexMeta.StoreWithHash)

	key := path.Join(indexName, fieldName)
	for i := 0; ; i++ {
		if i >= 10 {
			return fmt.Errorf("failed to close vector index %s, ref count is not zero", key)
		}

		vecIdxManager.locker.Lock(key)
		ok := vecIdxManager.cache.TryDelete(key)
		if ok {
			defer vecIdxManager.locker.Unlock(key)
			break
		}
		vecIdxManager.locker.Unlock(key)

		time.Sleep(time.Millisecond * (10 << i))
	}

	err := vecIdxManager.storage.Remove(storeName)
	if err != nil {
		return fmt.Errorf("failed to remove index %s/%s: %w", indexName, fieldName, err)
	}
	return nil
}

type VectorIndexLocker struct {
	mutex sync.Mutex
	locks map[string]*sync.Mutex
}

func (locker *VectorIndexLocker) Lock(key string) {
	locker.mutex.Lock()
	m, ok := locker.locks[key]
	if !ok {
		m = &sync.Mutex{}
		locker.locks[key] = m
	}
	locker.mutex.Unlock()

	m.Lock()
}

func (locker *VectorIndexLocker) Unlock(key string) {
	locker.mutex.Lock()
	m, ok := locker.locks[key]
	locker.mutex.Unlock()
	if !ok {
		return
	}

	m.Unlock()
}

type VectorIndexCache struct {
	mutex sync.Mutex
	items map[string]*VectorIndexCacheItem
	evict *list.List
	size  int64
}

type VectorIndexCacheItem struct {
	index VectorIndex
	elem  *list.Element
	atime time.Time
	size  int64
	ref   int
}

func (cache *VectorIndexCache) Acquire(key string) (VectorIndex, bool) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	item, ok := cache.items[key]
	if !ok {
		return nil, false
	}

	if item.elem != nil {
		cache.evict.Remove(item.elem)
		item.elem = nil
	}
	item.atime = time.Now()
	item.ref++

	return item.index, true
}

func (cache *VectorIndexCache) Release(key string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	item, ok := cache.items[key]
	if !ok {
		return
	}

	item.ref--
	if item.ref == 0 {
		size := item.index.Size()
		if size != item.size {
			cache.size += size - item.size
			item.size = size
		}
		item.elem = cache.evict.PushBack(item)
		cache.restrictSize()
	}
}

func (cache *VectorIndexCache) Set(key string, index VectorIndex) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	var item VectorIndexCacheItem
	item.index = index
	item.atime = time.Now()
	item.size = index.Size()
	item.ref = 1

	cache.items[key] = &item
	cache.size += item.size

	cache.restrictSize()
}

func (cache *VectorIndexCache) TryDelete(key string) bool {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	item, ok := cache.items[key]
	if !ok {
		return true
	}
	if item.ref != 0 {
		return false
	}

	cache.evictItem(item)

	return true
}

func (cache *VectorIndexCache) Clean() {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	now := time.Now()
	for _, item := range cache.items {
		// Update the size of the index in case it has changed since it was
		// last cached.
		size := item.index.Size()
		if size != item.size {
			cache.size += size - item.size
			item.size = size
		}

		if item.ref != 0 {
			continue
		}
		if now.Sub(item.atime) <= cacheTTL {
			continue
		}
		// Evict the item from the cache if it has exceeded the TTL.
		cache.evictItem(item)
	}

	cache.restrictSize()
}

func (cache *VectorIndexCache) restrictSize() {
	for cache.size > config.Global.VectorConfig.CacheSize && cache.evict.Len() > 0 {
		elem := cache.evict.Front()
		item := elem.Value.(*VectorIndexCacheItem)
		cache.evictItem(item)
	}
}

func (cache *VectorIndexCache) evictItem(item *VectorIndexCacheItem) {
	if item.elem != nil {
		cache.evict.Remove(item.elem)
		item.elem = nil
	}
	delete(cache.items, item.index.Name())
	cache.size -= item.size

	item.index.Free()
}

// cacheCleaner evicts bases which have exceeded the cacheTTL.
func cacheCleaner() {
	defer vecIdxManager.closer.Done()

	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			vecIdxManager.cache.Clean()

		case <-vecIdxManager.closer.HasBeenClosed():
			return
		}
	}
}

type sealTask struct {
	index    string
	field    string
	taskName string
}

// SealIndex
// submits a task to seal the growing segment of the index.
func SealIndex(zincIndexName string, field string) error {
	index, ok := GetIndex(zincIndexName)
	if !ok {
		return fmt.Errorf("zinc index not exists")
	}
	vecIndex, ok := index.GetVecIndex(field)
	if !ok {
		return ErrVecIndexNotExists
	}
	if vecIndex.TargetType != vector.IVFPQ {
		return fmt.Errorf("the vector index doesn't need to seal")
	}

	found := false
	for _, segMeta := range vecIndex.Segments {
		if segMeta.Status == vector.StatusGrowing &&
			atomic.LoadInt64(&segMeta.Count) >= config.Global.VectorConfig.IvfPqThreshold {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("the vector index doesn't need to seal")
	}

	taskName := path.Join(zincIndexName, field)
	vecIdxManager.sealedLock.RLock()
	if _, ok := vecIdxManager.sealTaskMp[taskName]; ok {
		vecIdxManager.sealedLock.RUnlock()
		return fmt.Errorf("the vector index is already sealing")
	}
	vecIdxManager.sealedLock.RUnlock()

	vecIdxManager.sealCh <- &sealTask{
		taskName: taskName,
		index:    zincIndexName,
		field:    field,
	}
	return nil
}

// backgroundSealCheck
// Traverse to check vector indexes and process vectors that need to be converted to ivf_pq.
func backgroundSealCheck() {
	defer vecIdxManager.closer.Done()
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case <-timer.C:
		case <-vecIdxManager.closer.HasBeenClosed():
			timer.Stop()
			return
		}
		for _, index := range ZINC_INDEX_LIST.List() {
			vecIndexes := index.GetVecIndexes()
			if len(vecIndexes) <= 0 {
				continue
			}
			for field, vecIndex := range vecIndexes {
				if vecIndex.TargetType != vector.IVFPQ {
					continue
				}

				find := false
				for _, segMeta := range vecIndex.Segments {
					if segMeta.Status == vector.StatusGrowing &&
						atomic.LoadInt64(&segMeta.Count) >= config.Global.VectorConfig.IvfPqThreshold {
						find = true
						break
					}
				}
				if !find {
					continue
				}

				taskName := path.Join(index.GetName(), field)
				vecIdxManager.sealedLock.RLock()
				if _, ok := vecIdxManager.sealTaskMp[taskName]; ok {
					vecIdxManager.sealedLock.RUnlock()
					continue
				}
				vecIdxManager.sealedLock.RUnlock()

				vecIdxManager.sealCh <- &sealTask{
					taskName: taskName,
					index:    index.GetName(),
					field:    field,
				}
			}

		}
	}
}

// backgroundSeal process sealed task
func backgroundSeal() {
	defer vecIdxManager.closer.Done()
	for {
		select {
		case <-vecIdxManager.closer.HasBeenClosed():
			return
		case task := <-vecIdxManager.sealCh:
			vecIdxManager.sealedLock.Lock()
			vecIdxManager.sealTaskMp[task.taskName] = struct{}{}
			vecIdxManager.sealedLock.Unlock()
			vecIndex, err := OpenVectorIndex(task.index, task.field)
			if err != nil {
				vecIdxManager.sealedLock.Lock()
				delete(vecIdxManager.sealTaskMp, task.taskName)
				vecIdxManager.sealedLock.Unlock()
				log.Error().Err(err).Msgf("process vector index %s segment sealed  err: get vector index err %s ", task.taskName, task.index)
				continue
			}
			start := time.Now()

			// we don't lock the whole vecIndex,
			// vecIndex will synchronize segment operations internally
			err = vecIndex.SealSeg()

			if err != nil {
				vecIdxManager.sealedLock.Lock()
				delete(vecIdxManager.sealTaskMp, task.taskName)
				vecIdxManager.sealedLock.Unlock()
				CloseVectorIndex(vecIndex)
				log.Error().Err(err).Msgf("failed to seal segment for vector index %s error: %s", task.taskName, err)
				continue
			}
			CloseVectorIndex(vecIndex)
			log.Debug().Msgf("seal segment for vector index %s finished. took %.2fs", task.taskName, time.Since(start).Seconds())

			vecIdxManager.sealedLock.Lock()
			delete(vecIdxManager.sealTaskMp, task.taskName)
			vecIdxManager.sealedLock.Unlock()
		}
	}
}

type BuildHNSWIndexTask struct {
	index string
	field string
}

func (task BuildHNSWIndexTask) less(other BuildHNSWIndexTask) bool {
	return cmp.Or(
		cmp.Compare(task.index, other.index),
		cmp.Compare(task.field, other.field),
	) < 0
}

func BuildHNSWIndex(index, field string) {
	task := BuildHNSWIndexTask{index: index, field: field}
	if _, ok := vecIdxManager.buildHNSWIndexTasks.Set(task); !ok {
		vecIdxManager.buildHNSWIndexChan <- task
	}
}

func backgroundBuildHNSWIndex() {
	defer vecIdxManager.closer.Done()
	for {
		select {
		case <-vecIdxManager.closer.HasBeenClosed():
			return

		case task := <-vecIdxManager.buildHNSWIndexChan:
			err := buildHNSWIndex(task)
			if err != nil {
				log.Error().Err(err).Msg("failed to create hnsw index")
				continue
			}
		}
	}
}

func buildHNSWIndex(task BuildHNSWIndexTask) error {
	defer vecIdxManager.buildHNSWIndexTasks.Delete(task)

	index, err := OpenVectorIndex(task.index, task.field)
	if err != nil {
		return fmt.Errorf("failed to get vector index %v %v: %w", task.index, task.field, err)
	}
	defer CloseVectorIndex(index)

	log.Debug().Msgf("start to build hnsw index for %v %v", task.index, task.field)

	start := time.Now()
	err = index.BuildHNSW(vecIdxManager.closer.Ctx())
	if err != nil {
		return err
	}

	log.Debug().Msgf("build hnsw index for %v %v finished. took %.2fs", task.index, task.field, time.Since(start).Seconds())

	return nil
}
