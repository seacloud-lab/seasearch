package core

import "C"
import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sync"
	"time"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/blugelabs/bluge"
	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
	"golang.org/x/sync/errgroup"
)

var (
	ErrVecIndexNotExists  = errors.New("index not found")
	ErrVecIndexCorruption = errors.New("index file is corrupted")
	ErrInvalidVec         = errors.New("invalid vectors")
	ErrInvalidArguments   = errors.New("invalid arguments")
)

type rebuildTask struct {
	index    string
	field    string
	taskName string
}

type VecIndexManager struct {
	cache         map[string]*VecIndex
	ready         map[string]chan struct{}
	rebuildTaskMp map[string]struct{}
	rebuildTaskCh chan *rebuildTask
	rebuildLock   sync.RWMutex
	lock          sync.Mutex
	storage       vector.ObjStore
	// path for saving temp file
	tmpDir string
	closer *z.Closer
}

var manager *VecIndexManager

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
	manager = &VecIndexManager{
		cache:         make(map[string]*VecIndex),
		ready:         make(map[string]chan struct{}),
		rebuildTaskMp: make(map[string]struct{}),
		rebuildTaskCh: make(chan *rebuildTask, 10),
		storage:       storage,
		closer:        z.NewCloser(3),
		tmpDir:        tmpDir,
	}

	go backGroundGC()

	go backGroundRebuildCheck()

	go backGroundRebuild()
}

func CloseVecIndexManager() {
	manager.closer.SignalAndWait()
}

// backGroundGC for free memory
func backGroundGC() {
	defer manager.closer.Done()
	var err error
	var previousFound bool
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case <-timer.C:
		case <-manager.closer.HasBeenClosed():
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

	manager.lock.Lock()

	var expired *VecIndex
	for _, index := range manager.cache {
		if index.refCount == 0 && time.Since(index.atime) > time.Hour {
			expired = index
			break
		}
	}
	if expired == nil {
		manager.lock.Unlock()
		log.Debug().Msg("end vec index gc, no expired index")
		return false, nil
	}

	if _, ok := manager.ready[expired.name]; ok {
		manager.lock.Unlock()
		log.Debug().Msgf("end vector index gc, index %v is opening/closing", expired.name)
		return true, nil
	}

	delete(manager.cache, expired.name)
	ready := make(chan struct{})
	manager.ready[expired.name] = ready
	manager.lock.Unlock()

	log.Debug().Msgf("free vector index memory %v", expired.name)
	expired.index.Delete()

	manager.lock.Lock()
	close(ready) // wakes all goroutines waiting on the channel
	delete(manager.ready, expired.name)
	manager.lock.Unlock()

	return true, nil
}

func GetVectorIndex(indexName string, fieldName string) (*VecIndex, error) {
	cachedName := path.Join(indexName, fieldName)
	for {
		manager.lock.Lock()
		if index, ok := manager.cache[cachedName]; ok {
			index.refCount++
			index.atime = time.Now()
			manager.lock.Unlock()
			return index, nil
		}

		if ready, ok := manager.ready[cachedName]; ok {
			manager.lock.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		manager.ready[cachedName] = ready
		manager.lock.Unlock()

		index := &VecIndex{
			field:         fieldName,
			zincIndexName: indexName,
			name:          cachedName,
			refCount:      1,
			atime:         time.Now(),
		}
		// not cached, we should check metadata
		vecIndexMeta, err := checkVecIndexMeta(indexName, fieldName)
		needCreate := false
		if err != nil {
			if errors.Is(err, ErrVecIndexNotExists) {
				needCreate = true
			} else {
				// other error
				manager.lock.Lock()
				close(ready)
				delete(manager.ready, cachedName)
				manager.lock.Unlock()
				return nil, err
			}
		}
		index.ref = vecIndexMeta
		if needCreate {
			err = index.init()
		} else {
			err = index.loadIndexFile()
		}
		if err != nil {
			manager.lock.Lock()
			close(ready)
			delete(manager.ready, cachedName)
			manager.lock.Unlock()
			return nil, fmt.Errorf("load faiss index error: %w", err)
		}

		manager.lock.Lock()
		close(ready) // wakes all goroutines waiting on the channel
		manager.cache[cachedName] = index
		delete(manager.ready, cachedName)
		manager.lock.Unlock()

		return index, nil
	}
}

func (v *VecIndex) init() error {
	var err error
	v.index, err = faiss.IndexFactory(v.ref.Dims, "IDMap,Flat", faiss.MetricL2)
	if err != nil {
		return fmt.Errorf("craete faiss error: %w", err)
	}
	return nil
}

// loadIndexFile load faiss into memory
func (v *VecIndex) loadIndexFile() error {
	filePath := path.Join(v.zincIndexName, v.field, getFileName())
	var localFile string
	var err error
	localFile, closer, err := manager.storage.LoadFile(filePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = closer.Close()
	}()
	if v.index == nil {
		v.index, err = faiss.ReadIndex(localFile, faiss.IOFlagReadOnly)
	}
	return err
}

// statIndex check index is exists in obj storage
// if there is a larger epoch exists, we should update metaData,
// this may be due to the successful writing of the index file, but the epoch was not updated successfully
func statIndex(indexName string, fieldName string) (bool, error) {
	exists, err := manager.storage.ExistsFile(path.Join(indexName, fieldName, getFileName()))
	return exists, err
}
func getFileName() string {
	return fmt.Sprintf("index%s", vector.IndexExt)
}

func checkVecIndexMeta(indexName, fieldName string) (*meta.VecIndex, error) {
	// get metadata
	indexMeta, ok := GetIndex(indexName)
	if !ok {
		log.Fatal().Msgf("try get zinc index %s for getting vector index, but zinc index not exists", indexName)
	}
	vecIndexMeta, ok := indexMeta.GetVecIndex(fieldName)
	if !ok {
		return nil, ErrVecIndexNotExists
	} else {
		exists, err := statIndex(indexName, fieldName)
		if err != nil {
			return nil, err
		}
		if !exists {
			if vecIndexMeta.Stored {
				return vecIndexMeta, ErrVecIndexCorruption
			}
			return vecIndexMeta, ErrVecIndexNotExists
		}
		return vecIndexMeta, nil
	}
}

func DeleteVecIndex(indexName string, fieldName string) error {
	vecIndex, err := GetVectorIndex(indexName, fieldName)
	if err == nil {
		vecIndex.Close(true)
	} else if !errors.Is(err, ErrVecIndexNotExists) && !errors.Is(err, ErrVecIndexCorruption) {
		return fmt.Errorf("failed to get vector index %s/%s: %w", indexName, fieldName, err)
	}
	err = manager.storage.Remove(path.Join(indexName, fieldName))
	if err != nil {
		return fmt.Errorf("failed to remove index %s/%s: %w", indexName, fieldName, err)
	}
	return nil
}

type vecDoc struct {
	docId string
	vec   []float32
}

// VecIndex wrap of faiss index
type VecIndex struct {
	ref *meta.VecIndex
	// name zincIndex/field
	name          string
	zincIndexName string
	field         string
	index         faiss.Index
	refCount      int
	atime         time.Time
	// lock for updating vector index concurrently
	lock sync.RWMutex
}

func (v *VecIndex) AddVectors(vectors [][]float32, ids []int64) error {
	d := v.index.D()
	finalVector := make([]float32, len(vectors)*d)
	if len(vectors) != len(ids) {
		log.Fatal().Err(ErrInvalidArguments).Msgf("len vectors is not equal to len ids")
		return ErrInvalidArguments
	}
	for k, vec := range vectors {
		if len(vec) != d {
			log.Fatal().Err(ErrInvalidVec).Msgf("vector dims is not equal to index setting")
			return ErrInvalidVec
		}
		for i, val := range vec {
			finalVector[k*d+i] = val
		}
	}

	v.lock.Lock()
	defer v.lock.Unlock()
	err := v.index.AddWithIDs(finalVector, ids)
	return err
}

func (v *VecIndex) RemoveIDs(ids []int64) (int, error) {
	selector, err := faiss.NewIDSelectorBatch(ids)
	if err != nil {
		return 0, err
	}
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.index.RemoveIDs(selector)
}

func (v *VecIndex) Save() error {
	// get metadata
	zincIndex, ok := GetIndex(v.zincIndexName)
	if !ok {
		log.Fatal().Msgf("try get zinc index %s for getting vector index, but zinc index not exists", v.zincIndexName)
	}

	v.lock.Lock()
	defer v.lock.Unlock()

	v.ref.Count = v.index.Ntotal()
	v.ref.Stored = true
	err := save(v.index, zincIndex.GetName(), v.field)
	if err != nil {
		return err
	}

	// update metaData
	return zincIndex.SaveVecIndexMeta(v.field, v.ref)
}

func save(idx faiss.Index, zincIndexName string, field string) error {
	f, err := os.CreateTemp(manager.tmpDir, "temp_index")
	if err != nil {
		return err
	}
	_ = f.Close()
	defer func() {
		_ = os.Remove(f.Name())
	}()

	err = faiss.WriteIndex(idx, f.Name())
	if err != nil {
		return err
	}
	name := path.Join(zincIndexName, field, getFileName())
	err = manager.storage.SaveFile(f.Name(), name)
	if err != nil {
		return err
	}
	return err
}

// Search query vector index,
// return a map docId->distance
// should reorder by distance
func (v *VecIndex) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	var ids []int64
	var err error
	if v.ref.Type == vector.IvfPQ {
		ps, err := faiss.NewParameterSpace()
		if err != nil {
			return nil, err
		}
		defer ps.Delete()

		if err := ps.SetIndexParameter(v.index, "nprobe", float64(nprobe)); err != nil {
			return nil, err
		}
	}
	var result = make(map[string]float32)

	var distance []float32
	v.lock.RLock()
	distance, ids, err = v.index.Search(vec, k)
	v.lock.RUnlock()
	if err != nil {
		return nil, err
	}

	if v.ref.Type == vector.Flat {
		for i, id := range ids {
			if id == -1 {
				continue
			}
			result[base62.Encode(id)] = distance[i]
		}
		return result, nil
	}
	docIds := make([]string, 0)
	for i := range ids {
		if ids[i] == -1 {
			continue
		}
		docIds = append(docIds, base62.Encode(ids[i]))
	}
	if len(docIds) == 0 {
		return result, nil
	}
	// for ivf_pq, we need to get vector and calculate the real distance
	q, err := query.TermsQuery(map[string]interface{}{
		"_id": docIds,
	}, nil)
	if err != nil {
		return nil, err
	}
	zincIndex, ok := GetIndex(v.zincIndexName)
	if !ok {
		return nil, fmt.Errorf("index %s not exists", v.zincIndexName)
	}
	readers, err := zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return nil, err
	}
	req := bluge.NewAllMatches(q)

	vectors, ids, err := getVectors(v.field, v.ref.Dims, req, readers...)
	if err != nil {
		return nil, err
	}
	for i := range ids {
		realVector := vectors[i*v.ref.Dims : (i+1)*v.ref.Dims]
		dis, err := L2Distance(vec, realVector)
		if err != nil {
			return nil, err
		}
		result[base62.Encode(ids[i])] = dis
	}
	return result, nil
}

func L2Distance(slice1, slice2 []float32) (float32, error) {
	if len(slice1) != len(slice2) {
		return 0, fmt.Errorf("invliad vectors")
	}

	var sum float32
	for i := 0; i < len(slice1); i++ {
		diff := slice1[i] - slice2[i]
		sum += diff * diff
	}

	return sum, nil
}

// createTempFlatIndex
// get vectors from bluge and build a temp flat index.
func createTempFlatIndex(zincIndex *Index, field string, d int) (faiss.Index, error) {
	// get documents
	readers, err := zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	q := bluge.NewMatchAllQuery()
	req := bluge.NewAllMatches(q)
	vectors, ids, err := getVectors(field, d, req, readers...)
	if err != nil {
		return nil, err
	}

	var idx faiss.Index

	idx, err = faiss.IndexFactory(d, "IDMap,Flat", faiss.MetricL2)
	if err != nil {
		return nil, err
	}
	err = idx.AddWithIDs(vectors, ids)
	if err != nil {
		idx.Delete()
		return nil, err
	}

	return idx, nil
}

// getQueryVectors
// get some vectors from bluge
func getQueryVectors(zincIndex *Index, field string, n int, dim int) ([][]float32, error) {
	// get documents
	readers, err := zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return nil, err
	}
	q := bluge.NewMatchAllQuery()
	req := bluge.NewTopNSearch(n, q)
	vectors, _, err := getVectors(field, dim, req, readers...)
	if err != nil {
		return nil, err
	}
	res := make([][]float32, 0, n)
	for i := 0; i < n; i++ {
		res = append(res, vectors[i*dim:(i+1)*dim])
	}
	return res, nil
}

func (v *VecIndex) Close(wait bool) {
	manager.lock.Lock()
	v.refCount--
	manager.lock.Unlock()

	if !wait {
		return
	}

	last := time.Now().Unix()
	onlyOnce := true

	for {
		manager.lock.Lock()
		if v.refCount == 0 {
			if _, ok := manager.ready[v.name]; ok {
				manager.lock.Unlock()
				return
			}
			ready := make(chan struct{})
			manager.ready[v.name] = ready
			delete(manager.cache, v.name)
			manager.lock.Unlock()

			// free index memory
			v.index.Delete()

			manager.lock.Lock()
			close(ready) // wakes all goroutines waiting on the channel
			delete(manager.ready, v.name)
			manager.lock.Unlock()
			return
		}
		manager.lock.Unlock()
		if onlyOnce && time.Now().Unix()-last > 10 {
			onlyOnce = false
			log.Warn().Msgf("can't close index %s, because the refcount of index is %d", v.name, v.refCount)
		}
		time.Sleep(time.Second)
	}
}

// RebuildIndex
// submit a rebuild vector index task
func RebuildIndex(zincIndexName string, field string) error {
	index, ok := GetIndex(zincIndexName)
	if !ok {
		return fmt.Errorf("zinc index not exists")
	}
	vecIndex, ok := index.GetVecIndex(field)
	if !ok {
		return ErrVecIndexNotExists
	}
	if vecIndex.TargetType != vector.IvfPQ || (vecIndex.Type == vector.Flat && vecIndex.Count < config.Global.VectorConfig.IvfPqThreshold) {
		return fmt.Errorf("the vector index doesn't need to rebuild")
	}

	taskName := path.Join(zincIndexName, field)
	manager.rebuildLock.RLock()
	if _, ok := manager.rebuildTaskMp[taskName]; ok {
		manager.rebuildLock.RUnlock()
		return fmt.Errorf("the vector index is already rebuilding")
	}
	manager.rebuildLock.RUnlock()

	manager.rebuildTaskCh <- &rebuildTask{
		taskName: taskName,
		index:    zincIndexName,
		field:    field,
	}
	return nil
}

// backGroundRebuildCheck
// Traverse to check vector indexes and process vectors that need to be converted to ivf_pq.
func backGroundRebuildCheck() {
	defer manager.closer.Done()
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case <-timer.C:
		case <-manager.closer.HasBeenClosed():
			timer.Stop()
			return
		}
		for _, index := range ZINC_INDEX_LIST.List() {
			vecIndexes := index.GetVecIndexes()
			if len(vecIndexes) <= 0 {
				continue
			}
			for field, vecIndex := range vecIndexes {
				if vecIndex.TargetType == vector.IvfPQ && vecIndex.Type == vector.Flat && vecIndex.Count >= config.Global.VectorConfig.IvfPqThreshold {
					taskName := path.Join(index.GetName(), field)
					manager.rebuildLock.RLock()
					if _, ok := manager.rebuildTaskMp[taskName]; ok {
						manager.rebuildLock.RUnlock()
						continue
					}
					manager.rebuildLock.RUnlock()

					manager.rebuildTaskCh <- &rebuildTask{
						taskName: taskName,
						index:    index.GetName(),
						field:    field,
					}
				}
			}
		}
	}
}

// backGroundRebuild process rebuild task
func backGroundRebuild() {
	defer manager.closer.Done()
	for {
		select {
		case <-manager.closer.HasBeenClosed():
			return
		case task := <-manager.rebuildTaskCh:
			manager.rebuildLock.Lock()
			manager.rebuildTaskMp[task.taskName] = struct{}{}
			manager.rebuildLock.Unlock()
			vecIndex, err := GetVectorIndex(task.index, task.field)
			if err != nil {
				manager.rebuildLock.Lock()
				delete(manager.rebuildTaskMp, task.taskName)
				manager.rebuildLock.Unlock()
				log.Error().Err(err).Msgf("rebuild vector index %s err: get vector index err %s ", task.taskName, task.index)
				continue
			}
			start := time.Now()
			err = vecIndex.rebuild()
			if err != nil {
				manager.rebuildLock.Lock()
				delete(manager.rebuildTaskMp, task.taskName)
				manager.rebuildLock.Unlock()
				vecIndex.Close(false)
				log.Error().Err(err).Msgf("rebuild vector index %s error: %s", task.taskName, err)
				continue
			}
			vecIndex.Close(false)
			log.Debug().Msgf("%s rebuild finish, took %f s", task.taskName, time.Since(start).Seconds())

			manager.rebuildLock.Lock()
			delete(manager.rebuildTaskMp, task.taskName)
			manager.rebuildLock.Unlock()
		}
	}
}

// rebuild
// rebuild index file and update metadata
func (v *VecIndex) rebuild() error {
	// get metadata
	zincIndex, ok := GetIndex(v.zincIndexName)
	if !ok {
		return fmt.Errorf("index %s is not exists", v.zincIndexName)
	}

	t := vector.Flat
	if v.ref.Count >= config.Global.VectorConfig.IvfPqThreshold {
		t = vector.IvfPQ
	}

	return rebuildVecIndex(zincIndex, v, t)
}

// rebuildVecIndex rebuild and save index file
func rebuildVecIndex(zincIndex *Index, vecIndex *VecIndex, targetType string) error {
	// get documents
	readers, err := zincIndex.GetReaders(0, 0)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	if err != nil {
		return err
	}

	d := vecIndex.ref.Dims

	q := bluge.NewMatchAllQuery()
	req := bluge.NewAllMatches(q)
	vectors, ids, err := getVectors(vecIndex.field, d, req, readers...)
	if err != nil {
		return err
	}
	if len(vectors) == 0 {
		// do nothing
		return nil
	}

	var idx faiss.Index
	nlist := 0
	if targetType == vector.IvfPQ {
		nlist = int(4 * math.Sqrt(float64(len(ids))))
		idx, err = faiss.IndexFactory(d, fmt.Sprintf("IVF%d,PQ%dx%d", nlist, vecIndex.ref.M, vecIndex.ref.NBits), faiss.MetricL2)
		if err != nil {
			return err
		}
		err = idx.Train(vectors)
		if err != nil {
			idx.Delete()
			return err
		}
	} else {
		idx, err = faiss.IndexFactory(d, "IDMap,Flat", faiss.MetricL2)
		if err != nil {
			return err
		}
	}
	err = idx.AddWithIDs(vectors, ids)
	if err != nil {
		idx.Delete()
		return err
	}

	// add index
	vecIndex.lock.Lock()
	err = save(idx, zincIndex.GetName(), vecIndex.field)
	vecIndex.ref.Type = targetType
	vecIndex.ref.NList = nlist
	vecIndex.ref.Count = idx.Ntotal()

	// free old index
	if vecIndex.index != nil {
		vecIndex.index.Delete()
	}
	vecIndex.index = idx
	vecIndex.lock.Unlock()
	if err != nil {
		return err
	}
	return zincIndex.SaveVecIndexMeta(vecIndex.field, vecIndex.ref)
}

// getVectors get vector from bluge
func getVectors(field string, d int, searchReq bluge.SearchRequest, readers ...*bluge.Reader) ([]float32, []int64, error) {
	ch := make(chan *vecDoc, len(readers)*10)
	eg := &errgroup.Group{}
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	for _, reader := range readers {
		r := reader
		eg.Go(func() error {
			dmi, err := r.Search(context.Background(), searchReq)
			if err != nil {
				return err
			}
			for next, err := dmi.Next(); err == nil && next != nil; next, err = dmi.Next() {
				var id string
				var vec []float32
				ok := false
				err = next.VisitStoredFields(func(f string, value []byte) bool {
					if f == "_id" {
						id = string(value)
						return true
					}
					if f == field {
						var v = make([]float32, d)
						err := json.Unmarshal(value, &v)
						if err != nil {
							return false
						}
						vec = v
						ok = true
						return true
					}
					if id != "" && ok {
						return false
					}
					return true
				})
				ch <- &vecDoc{
					vec:   vec,
					docId: id,
				}
				if err != nil {
					return err
				}
			}
			return err
		})
	}

	var vectors []float32
	var ids []int64
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		docs := make([]*vecDoc, 0)
		for doc := range ch {
			docs = append(docs, doc)
		}
		vectors = make([]float32, len(docs)*d)
		ids = make([]int64, len(docs))
		for i, doc := range docs {
			if len(doc.vec) != d {
				log.Fatal().Err(ErrInvalidVec).Msgf("vector dims is not equal to index setting")
				return
			}
			for j, val := range doc.vec {
				vectors[i*d+j] = val
			}
			ids[i] = base62.Decode(doc.docId)
		}
	}()

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	close(ch)
	wg.Wait()

	return vectors, ids, nil
}
