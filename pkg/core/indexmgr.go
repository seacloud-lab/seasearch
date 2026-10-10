package core

import (
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils/hash/rendezvous"

	"github.com/blugelabs/bluge/analysis"
)

var (
	ErrIndexNotFound error = fmt.Errorf("index not found: %w", errors.ErrKeyNotFound)

	IndexMgr = newIndexManager()
)

// IndexManager manages the lifecycle of indexes, including loading, caching,
// and deleting indexes.
type IndexManager struct {
	mutex sync.Mutex
	cache map[string]*Index
	ready map[string]chan struct{}
}

func newIndexManager() *IndexManager {
	var mgr IndexManager
	mgr.cache = make(map[string]*Index)
	mgr.ready = make(map[string]chan struct{})
	return &mgr
}

// Get retrieves an index by name. If the index is not already cached, it will
// be loaded from metadata.
func (mgr *IndexManager) Get(name string) (*Index, error) {
	if !cluster.AssignCheck(name) {
		return nil, ErrIndexServerMismatch
	}

	for {
		mgr.mutex.Lock()

		if index, ok := mgr.cache[name]; ok {
			mgr.mutex.Unlock()
			return index, nil
		}

		if ready, ok := mgr.ready[name]; ok {
			mgr.mutex.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		mgr.ready[name] = ready
		mgr.mutex.Unlock()

		index, err := mgr.getIndex(name)
		if err != nil {
			mgr.mutex.Lock()
			close(ready)
			delete(mgr.ready, name)
			mgr.mutex.Unlock()
			return nil, fmt.Errorf("failed to load index: %w", err)
		}

		mgr.mutex.Lock()
		close(ready)
		delete(mgr.ready, name)
		mgr.cache[name] = index
		mgr.mutex.Unlock()

		return index, nil
	}
}

func (mgr *IndexManager) getIndex(name string) (*Index, error) {
	data, err := metadata.Index.Get(name)
	if errors.Is(err, errors.ErrKeyNotFound) {
		return nil, ErrIndexNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}
	index, err := formatIndex(data)
	if err != nil {
		return nil, fmt.Errorf("failed to format index: %w", err)
	}
	return index, nil
}

// GetOrCreate retrieves an index by name, creating it if it does not exist. It
// returns the index, a boolean indicating whether the index already existed,
// and an error if any occurred during the process.
func (mgr *IndexManager) GetOrCreate(name string) (*Index, bool, error) {
	if !cluster.AssignCheck(name) {
		return nil, false, ErrIndexServerMismatch
	}

	for {
		mgr.mutex.Lock()

		if index, ok := mgr.cache[name]; ok {
			mgr.mutex.Unlock()
			return index, true, nil
		}

		if ready, ok := mgr.ready[name]; ok {
			mgr.mutex.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		mgr.ready[name] = ready
		mgr.mutex.Unlock()

		index, exist, err := mgr.getOrCreateIndex(name)
		if err != nil {
			mgr.mutex.Lock()
			close(ready)
			delete(mgr.ready, name)
			mgr.mutex.Unlock()
			return nil, false, fmt.Errorf("failed to get or create index: %w", err)
		}

		mgr.mutex.Lock()
		close(ready)
		delete(mgr.ready, name)
		mgr.cache[name] = index
		mgr.mutex.Unlock()

		return index, exist, nil
	}
}

func (mgr *IndexManager) getOrCreateIndex(name string) (*Index, bool, error) {
	zincIndex, err := metadata.Index.Get(name)
	if errors.Is(err, errors.ErrKeyNotFound) {
		// do nothing
	} else if err != nil {
		return nil, false, fmt.Errorf("failed to get metadata: %w", err)
	} else {
		index, err := formatIndex(zincIndex)
		if err != nil {
			return nil, false, fmt.Errorf("failed to format index: %w", err)
		}
		return index, true, nil
	}

	index, err := NewIndex(name)
	if err != nil {
		return nil, false, err
	}
	checkIndex(index)
	data, err := index.MarshalJSON()
	if err != nil {
		return nil, false, err
	}
	err = metadata.Index.Set(index.GetName(), data)
	if err != nil {
		return nil, false, fmt.Errorf("failed to set metadata: %w", err)
	}
	return index, false, nil
}

// Store saves an index to the metadata store and updates the cache. If an
// index with the same name already exists in the cache, it will be replaced.
func (mgr *IndexManager) Store(index *Index) error {
	name := index.GetName()
	for {
		mgr.mutex.Lock()

		if ready, ok := mgr.ready[name]; ok {
			mgr.mutex.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		mgr.ready[name] = ready
		mgr.mutex.Unlock()

		err := mgr.storeIndex(index)
		if err != nil {
			mgr.mutex.Lock()
			close(ready)
			delete(mgr.ready, name)
			mgr.mutex.Unlock()
			return fmt.Errorf("failed to delete index: %w", err)
		}

		mgr.mutex.Lock()
		close(ready)
		delete(mgr.ready, name)
		if cached, ok := mgr.cache[name]; ok && cached != index {
			delete(mgr.cache, name)
		}
		mgr.mutex.Unlock()

		return nil
	}
}

func (mgr *IndexManager) storeIndex(index *Index) error {
	checkIndex(index)
	data, err := index.MarshalJSON()
	if err != nil {
		return err
	}
	err = metadata.Index.Set(index.GetName(), data)
	if err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}
	return nil
}

func (mgr *IndexManager) GetCached() []*Index {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	var items []*Index
	for _, item := range mgr.cache {
		items = append(items, item)
	}
	return items
}

func (mgr *IndexManager) DropCache(name string) (*Index, bool) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	index, ok := mgr.cache[name]
	if ok {
		delete(mgr.cache, name)
	}
	return index, ok
}

// Delete removes an index from the metadata store and the cache.
func (mgr *IndexManager) Delete(name string) error {
	if !cluster.AssignCheck(name) {
		return ErrIndexServerMismatch
	}

	for {
		mgr.mutex.Lock()

		if ready, ok := mgr.ready[name]; ok {
			mgr.mutex.Unlock()
			<-ready
			continue
		}

		ready := make(chan struct{})
		mgr.ready[name] = ready
		mgr.mutex.Unlock()

		err := mgr.deleteIndex(name)
		if err != nil {
			mgr.mutex.Lock()
			close(ready)
			delete(mgr.ready, name)
			mgr.mutex.Unlock()
			return fmt.Errorf("failed to delete index: %w", err)
		}

		mgr.mutex.Lock()
		close(ready)
		delete(mgr.ready, name)
		delete(mgr.cache, name)
		mgr.mutex.Unlock()

		return nil
	}
}

func (mgr *IndexManager) deleteIndex(name string) error {
	mgr.mutex.Lock()
	index, cached := mgr.cache[name]
	mgr.mutex.Unlock()

	if !cached {
		var err error
		index, err = mgr.getIndex(name)
		if errors.Is(err, ErrIndexNotFound) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to get index: %w", err)
		}
	}
	if cached {
		if err := index.Close(); err != nil {
			return fmt.Errorf("failed to close index: %w", err)
		}
	}

	vecIndexes := index.GetVecIndexes()
	for vecIndex := range vecIndexes {
		err := deleteVecIndexFromIndex(index, vecIndex)
		if err != nil {
			if errors.Is(err, ErrVecIndexNotExists) {
				continue
			}
			return fmt.Errorf("delete vec index err: %w", err)
		}
	}
	err := os.RemoveAll(path.Join(config.Global.DataPath, vector.VecPrefix, index.GetStoreName()))
	if err != nil {
		return fmt.Errorf("delete vec index err: %w", err)
	}
	switch index.ref.StorageType {
	case "oss":
		err = directory.RemoveOssIndex(index.GetStoreName())
	case "s3":
		err = directory.RemoveS3Index(index.GetStoreName())
	default:
		dataPath := config.Global.DataPath
		err = os.RemoveAll(dataPath + "/" + index.GetStoreName())
	}
	if err != nil {
		return fmt.Errorf("remove index err: %w", err)
	}
	err = metadata.Index.Delete(name)
	if err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}
	return nil
}

func checkIndex(index *Index) {
	index.lock.Lock()

	if index.ref.Settings == nil {
		index.ref.Settings = new(meta.IndexSettings)
	}
	if index.ref.Mappings == nil {
		// set default mappings
		index.ref.Mappings = meta.NewMappings()
		index.ref.Mappings.SetProperty(meta.TimeFieldName, meta.NewProperty("date"))
	}
	if index.analyzers == nil {
		index.analyzers = make(map[string]*analysis.Analyzer)
	}

	index.lock.Unlock()
}

func formatIndex(readIndex *meta.Index) (*Index, error) {
	index := new(Index)
	index.ref = new(meta.Index)
	index.ref.Name = readIndex.Name
	index.ref.Version = readIndex.Version
	index.ref.StorageType = readIndex.StorageType
	index.ref.Settings = readIndex.Settings
	index.ref.Mappings = readIndex.Mappings
	index.ref.Stats = readIndex.Stats
	index.ref.VecIndexes = readIndex.VecIndexes
	//The new index field from the 9.0.2 version is set to true, and the hash is used as the storage path
	index.ref.StoreWithHash = readIndex.StoreWithHash

	// init shards
	index.ref.ShardNum = readIndex.ShardNum
	index.ref.Shards = make(map[string]*meta.IndexShard)
	for id := range readIndex.Shards {
		index.ref.Shards[id] = &meta.IndexShard{
			ID:       readIndex.Shards[id].ID,
			NodeID:   readIndex.Shards[id].NodeID,
			ShardNum: readIndex.Shards[id].ShardNum,
			Stats:    readIndex.Shards[id].Stats,
		}
		index.ref.Shards[id].Shards = make([]*meta.IndexSecondShard, index.ref.Shards[id].ShardNum)
		for j := range readIndex.Shards[id].Shards {
			index.ref.Shards[id].Shards[j] = &meta.IndexSecondShard{
				ID:    readIndex.Shards[id].Shards[j].ID,
				Stats: readIndex.Shards[id].Shards[j].Stats,
			}
		}
	}

	// init shards wrapper
	index.shardNum = index.ref.ShardNum
	index.shards = make(map[string]*IndexShard, index.shardNum)
	for id := range index.ref.Shards {
		index.shards[id] = &IndexShard{
			root: index,
			ref:  index.ref.Shards[id],
			name: index.ref.Name + "/" + index.ref.Shards[id].ID,
		}
		index.shards[id].shards = make([]*IndexSecondShard, index.ref.Shards[id].ShardNum)
		for j := range index.ref.Shards[id].Shards {
			index.shards[id].shards[j] = &IndexSecondShard{
				root: index,
				ref:  index.ref.Shards[id].Shards[j],
			}
		}
	}

	// init shards hashing
	index.shardHashing = rendezvous.New()
	for id := range index.shards {
		index.shardHashing.Add(id)
	}

	// load index analysis
	if index.ref.Settings != nil && index.ref.Settings.Analysis != nil {
		var err error
		index.analyzers, err = zincanalysis.RequestAnalyzer(index.ref.Settings.Analysis)
		if err != nil {
			return nil, errors.New(errors.ErrorTypeRuntimeException, "parse stored analysis error").Cause(err)
		}
	}

	return index, nil
}
