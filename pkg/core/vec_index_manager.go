package core

import "C"
import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
)

var (
	ErrVecIndexNotExists  = errors.New("index not found")
	ErrVecIndexCorruption = errors.New("index file is corrupted")
	ErrInvalidVec         = errors.New("invalid vectors")
	ErrInvalidArguments   = errors.New("invalid arguments")
)

type sealTask struct {
	index    string
	field    string
	taskName string
}

type VecIndexManager struct {
	cache      map[string]VectorIndex
	ready      map[string]chan struct{}
	sealTaskMp map[string]struct{}
	sealCh     chan *sealTask
	sealedLock sync.RWMutex
	lock       sync.Mutex
	storage    vector.ObjStore
	// path for saving temp file
	tmpDir string
	closer *z.Closer
}

var vecIdxManager *VecIndexManager

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
	vecIdxManager = &VecIndexManager{
		cache:      make(map[string]VectorIndex),
		ready:      make(map[string]chan struct{}),
		sealTaskMp: make(map[string]struct{}),
		sealCh:     make(chan *sealTask, 10),
		storage:    storage,
		closer:     z.NewCloser(3),
		tmpDir:     tmpDir,
	}

	go backgroundGC()

	go backgroundSealCheck()

	go backgroundSeal()

	go lazyCloseSegmentWriter()
}

func CloseVecIndexManager() {
	vecIdxManager.closer.SignalAndWait()
}

// backgroundGC for free memory
func backgroundGC() {
	defer vecIdxManager.closer.Done()
	var err error
	var previousFound bool
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case <-timer.C:
		case <-vecIdxManager.closer.HasBeenClosed():
			timer.Stop()
			return
		}

		if previousFound, err = execGC(); err != nil {
			log.Error().Err(err).Msgf("background vector index GC err: ")
		}

		if previousFound {
			timer.Reset(time.Minute)
		} else {
			timer.Reset(time.Hour)
		}
	}
}

func execGC() (bool, error) {
	log.Debug().Msg("start vector index gc")

	vecIdxManager.lock.Lock()

	var expired VectorIndex
	for _, index := range vecIdxManager.cache {
		if index.RefCount() == 0 && time.Since(index.ATime()) > time.Hour {
			expired = index
			break
		}
	}
	if expired == nil {
		vecIdxManager.lock.Unlock()
		log.Debug().Msg("end vec index gc, no expired index")
		return false, nil
	}

	if _, ok := vecIdxManager.ready[expired.Name()]; ok {
		vecIdxManager.lock.Unlock()
		log.Debug().Msgf("end vector index gc, index %v is opening/closing", expired.Name())
		return true, nil
	}

	delete(vecIdxManager.cache, expired.Name())
	ready := make(chan struct{})
	vecIdxManager.ready[expired.Name()] = ready
	vecIdxManager.lock.Unlock()

	log.Debug().Msgf("free vector index memory %v", expired.Name())
	expired.Free()

	vecIdxManager.lock.Lock()
	close(ready) // wakes all goroutines waiting on the channel
	delete(vecIdxManager.ready, expired.Name())
	vecIdxManager.lock.Unlock()

	return true, nil
}

// GetVectorIndex
// The caller should always call the CloseVectorIndex after obtaining the vector index.
func GetVectorIndex(indexName string, fieldName string) (VectorIndex, error) {
	cachedName := path.Join(indexName, fieldName)
	for {
		vecIdxManager.lock.Lock()
		if index, ok := vecIdxManager.cache[cachedName]; ok {
			index.AddRef()
			vecIdxManager.lock.Unlock()
			return index, nil
		}

		if ready, ok := vecIdxManager.ready[cachedName]; ok {
			vecIdxManager.lock.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		vecIdxManager.ready[cachedName] = ready
		vecIdxManager.lock.Unlock()

		index, err := getVectorIndex(fieldName, indexName)
		if err != nil {
			vecIdxManager.lock.Lock()
			close(ready)
			delete(vecIdxManager.ready, cachedName)
			vecIdxManager.lock.Unlock()
			return nil, fmt.Errorf("load index error: %w", err)
		}

		vecIdxManager.lock.Lock()
		close(ready) // wakes all goroutines waiting on the channel
		vecIdxManager.cache[cachedName] = index
		delete(vecIdxManager.ready, cachedName)
		vecIdxManager.lock.Unlock()

		return index, nil
	}
}

// CloseVectorIndex
// Close the vector index.
func CloseVectorIndex(idx VectorIndex, wait bool) {
	vecIdxManager.lock.Lock()
	idx.ReduceRef()
	vecIdxManager.lock.Unlock()
	if !wait {
		return
	}

	last := time.Now().Unix()
	onlyOnce := true

	for {
		vecIdxManager.lock.Lock()
		if idx.RefCount() == 0 {
			if _, ok := vecIdxManager.ready[idx.Name()]; ok {
				vecIdxManager.lock.Unlock()
				return
			}
			ready := make(chan struct{})
			vecIdxManager.ready[idx.Name()] = ready
			delete(vecIdxManager.cache, idx.Name())
			vecIdxManager.lock.Unlock()

			// free index memory
			idx.Free()

			vecIdxManager.lock.Lock()
			close(ready) // wakes all goroutines waiting on the channel
			delete(vecIdxManager.ready, idx.Name())
			vecIdxManager.lock.Unlock()
			return
		}
		vecIdxManager.lock.Unlock()
		if onlyOnce && time.Now().Unix()-last > 10 {
			onlyOnce = false
			log.Warn().Msgf("can't close index %s, because the refCount of index is %d", idx.Name(), idx.RefCount())
		}
		time.Sleep(time.Second)
	}
}

func getVectorIndex(field, zincIndexName string) (VectorIndex, error) {
	// get metadata
	zincIndex, ok := GetIndex(zincIndexName)
	if !ok {
		return nil, fmt.Errorf("try get zinc index %s for getting vector field %s, but zinc index not exists", zincIndexName, field)
	}
	vecIndexMeta, ok := zincIndex.GetVecIndex(field)
	if !ok {
		// the vector index metadata should always exist unless the field not exists, or it's not a vector field.
		return nil, ErrVecIndexNotExists
	}

	return MakeVecIndex(zincIndex, field, vecIndexMeta)
}

func DeleteVecIndex(indexName string, fieldName string) error {
	vecIndex, err := GetVectorIndex(indexName, fieldName)
	if err == nil {
		CloseVectorIndex(vecIndex, true)
	} else if !errors.Is(err, ErrVecIndexNotExists) && !errors.Is(err, ErrVecIndexCorruption) {
		return fmt.Errorf("failed to get vector index %s/%s: %w", indexName, fieldName, err)
	}
	err = vecIdxManager.storage.Remove(path.Join(indexName, fieldName))
	if err != nil {
		return fmt.Errorf("failed to remove index %s/%s: %w", indexName, fieldName, err)
	}
	return nil
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
	if vecIndex.TargetType != vector.IvfPQ {
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
				if vecIndex.TargetType != vector.IvfPQ {
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
			vecIndex, err := GetVectorIndex(task.index, task.field)
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
				CloseVectorIndex(vecIndex, false)
				log.Error().Err(err).Msgf("failed to seal segment for vector index %s error: %s", task.taskName, err)
				continue
			}
			CloseVectorIndex(vecIndex, false)
			log.Debug().Msgf("seal segment for vector index %s finished. took %.2fs", task.taskName, time.Since(start).Seconds())

			vecIdxManager.sealedLock.Lock()
			delete(vecIdxManager.sealTaskMp, task.taskName)
			vecIdxManager.sealedLock.Unlock()
		}
	}
}
