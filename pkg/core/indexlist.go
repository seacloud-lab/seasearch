/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package core

import (
	"crypto/md5"
	"encoding/hex"
	stderrors "errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

var (
	ZINC_INDEX_LIST   IndexList
	indexUpdateCloser = z.NewCloser(1)
	version           []byte
)

type IndexList struct {
	Indexes       map[string]*Index
	invalidated   map[string]bool
	lock          sync.RWMutex
	lifecycleLock sync.Mutex
	gcCloser      *z.Closer
}

func InitIndexList() {
	// check version
	version, _ = metadata.KV.Get("version")
	if version == nil {
		// version have version from v0.2.5
		// so if no version, it should be <= v0.2.4
		version = []byte("v0.2.4")
	}

	// start loading index
	ZINC_INDEX_LIST.Indexes = make(map[string]*Index)
	ZINC_INDEX_LIST.invalidated = make(map[string]bool)
	ZINC_INDEX_LIST.gcCloser = z.NewCloser(1)

	go ZINC_INDEX_LIST.StartGC()
	go LazyCloseSecondIndexShardWriters()

	// update version
	if string(version) != meta.Version {
		err := metadata.KV.Set("version", []byte(meta.Version))
		if err != nil {
			log.Error().Err(err).Msg("Error set version")
		}
	}

	var err error
	ZINC_INDEX_ALIAS_LIST.Aliases, err = metadata.Alias.Get()
	if err != nil {
		log.Fatal().Err(err).Msg("Error loading alias")
	}

	if !config.Global.Cluster.Enable {
		return
	}

	go watchIndexUpdate()
}

func (t *IndexList) Add(index *Index) {
	t.lock.Lock()
	if t.Indexes == nil {
		t.Indexes = make(map[string]*Index)
	}
	t.Indexes[index.GetName()] = index
	index.atime = time.Now().Unix()
	t.lock.Unlock()
}

func (t *IndexList) loadLocked(name string) (*Index, error) {
	if t.invalidated == nil {
		t.invalidated = make(map[string]bool)
	}
	if deleting, invalidated := t.invalidated[name]; invalidated {
		if deleting {
			return nil, zincerrors.ErrKeyNotFound
		}
		delete(t.invalidated, name)
	}
	if idx, ok := t.Get(name); ok {
		return idx, nil
	}

	readIndex, err := metadata.Index.Get(name)
	if err != nil {
		return nil, err
	}
	index, err := formatIndex(readIndex)
	if err != nil {
		return nil, err
	}
	if !cluster.AssignCheck(name) {
		return nil, ErrIndexServerMismatch
	}
	delete(t.invalidated, name)
	t.Add(index)
	return index, nil
}

func (t *IndexList) Load(name string) (*Index, error) {
	if !cluster.AssignCheck(name) {
		return nil, ErrIndexServerMismatch
	}
	if idx, ok := t.Get(name); ok {
		return idx, nil
	}

	t.lifecycleLock.Lock()
	defer t.lifecycleLock.Unlock()
	if t.invalidated == nil {
		t.invalidated = make(map[string]bool)
	}
	return t.loadLocked(name)
}

func (t *IndexList) Get(name string) (*Index, bool) {
	t.lock.RLock()
	idx, ok := t.Indexes[name]
	t.lock.RUnlock()
	if ok {
		atomic.StoreInt64(&idx.atime, time.Now().Unix())
	}
	return idx, ok
}

func (t *IndexList) GetOrCreate(name string) (*Index, bool, error) {
	idx, ok := t.Get(name)
	if ok {
		return idx, true, nil
	}
	t.lifecycleLock.Lock()
	defer t.lifecycleLock.Unlock()
	if deleting, invalidated := t.invalidated[name]; invalidated {
		if deleting {
			if _, err := metadata.Index.Get(name); !stderrors.Is(err, zincerrors.ErrKeyNotFound) {
				return nil, false, zincerrors.ErrKeyNotFound
			}
		}
		delete(t.invalidated, name)
	}
	// maybe someone else created it while we were waiting for the lock
	idx, ok = t.Get(name)
	if ok {
		return idx, true, nil
	}
	readIndex, err := metadata.Index.Get(name)
	if err == nil {
		idx, err = formatIndex(readIndex)
		if err != nil {
			return nil, false, err
		}
		if !cluster.AssignCheck(name) {
			return nil, false, ErrIndexServerMismatch
		}
		delete(t.invalidated, name)
		t.Add(idx)
		return idx, true, nil
	}
	if !stderrors.Is(err, zincerrors.ErrKeyNotFound) {
		return nil, false, err
	}
	// okay, let's create new index
	idx, err = NewIndex(name)
	if err != nil {
		return nil, false, err
	}
	// check index
	checkIndex(idx)
	if err = storeIndex(idx); err != nil {
		return nil, false, err
	}
	delete(t.invalidated, name)
	// cache it
	t.Add(idx)
	return idx, false, nil
}

func (t *IndexList) Delete(name string) {
	t.lock.Lock()
	if idx, ok := t.Indexes[name]; ok {
		if err := idx.Close(); err != nil {
			log.Error().Err(err).Msgf("Error Delete index[%s]", name)
		}
	}
	delete(t.Indexes, name)
	t.lock.Unlock()
}

func (t *IndexList) Remove(name string) (*Index, bool) {
	t.lock.Lock()
	index, ok := t.Indexes[name]
	delete(t.Indexes, name)
	t.lock.Unlock()
	return index, ok
}

func (t *IndexList) Len() int {
	t.lock.RLock()
	n := len(t.Indexes)
	t.lock.RUnlock()
	return n
}

func (t *IndexList) List() []*Index {
	t.lock.RLock()
	indexes := make([]*Index, 0, len(t.Indexes))
	for _, index := range t.Indexes {
		indexes = append(indexes, index)
	}
	t.lock.RUnlock()
	return indexes
}

func (t *IndexList) ListMap() map[string]*Index {
	items := t.List()

	indexes := make(map[string]*Index, len(items))
	for _, index := range items {
		indexes[index.ref.Name] = index
	}

	return indexes
}

func (t *IndexList) ListStat() []*Index {
	items := t.List()
	return items
}

func (t *IndexList) ListName() []string {
	items := t.List()
	sort.Slice(items, func(i, j int) bool {
		return items[i].GetName() < items[j].GetName()
	})

	names := make([]string, 0, len(items))
	for _, index := range items {
		names = append(names, index.GetName())
	}

	return names
}

func (t *IndexList) Close() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.gcCloser.SignalAndWait()

	eg := errgroup.Group{}
	eg.SetLimit(config.Global.Shard.GoroutineNum)
	for _, index := range t.Indexes {
		index := index
		eg.Go(func() error {
			return index.Close()
		})
	}
	return eg.Wait()
}

func (t *IndexList) StartGC() {
	defer t.gcCloser.Done()

	ticker := time.NewTicker(600 * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-t.gcCloser.HasBeenClosed():
			ticker.Stop()
			return
		}
		err := t.GC()
		if err != nil {
			log.Error().Err(err).Msg("Index GC err: ")
		}
	}
}

const indexExpire = 24 * 60 * 60

// GC auto close unused indexes when inactive for a long time (24h)
// In order to avoid error caused by close an index that is in using,
// the expiration time is 24 hours.
// It can be assumed that the index which is not used after this time can be safely close.
func (t *IndexList) GC() error {
	return nil
}

// LazyCloseSecondIndexShardWriters
// We close all unused indexSecondShard writers in a delayed manner,
// which is to ensure that the finishing work inside the writer can proceed normally,
// and to ensure that the underlying resources can be released.
func LazyCloseSecondIndexShardWriters() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		secondShardList := make([]*tempSecondShd, 0)
		for _, idx := range ZINC_INDEX_LIST.List() {
			for _, shd := range idx.shards {
				shd.lock.RLock()
				for _, secondShd := range shd.shards {
					secondShardList = append(secondShardList, &tempSecondShd{
						secondShard: secondShd,
						indexShard:  shd,
					})
				}
				shd.lock.RUnlock()
			}
		}

		for _, temp := range secondShardList {
			shard := temp.secondShard
			shard.lock.RLock()
			w := shard.writer
			refCount := shard.writerRefcount
			closeTime := shard.closeTime
			shard.lock.RUnlock()

			if refCount == 0 && time.Since(closeTime) > 5*time.Minute && w != nil {
				shard.lock.Lock()
				shard.writer = nil
				shard.lock.Unlock()
				_ = w.Close()
				log.Debug().Msgf("lazy close second shard %s %d", temp.indexShard.name, shard.ref.ID)
			}
		}
	}
}

func CloseIndexList() {
	if !config.Global.Cluster.Enable {
		return
	}
	indexUpdateCloser.SignalAndWait()
}

func watchIndexUpdate() {
	defer indexUpdateCloser.Done()
	for {
		select {
		case <-indexUpdateCloser.HasBeenClosed():
			return
		case unassignMap := <-cluster.UnassignChan:
			err := updateIndexList(unassignMap)
			if err != nil {
				log.Error().Err(err).Msg("cannot update memory index list")
			}
		}
	}
}

func updateIndexList(unassignMap map[string]struct{}) error {
	ZINC_INDEX_LIST.lifecycleLock.Lock()
	if ZINC_INDEX_LIST.invalidated == nil {
		ZINC_INDEX_LIST.invalidated = make(map[string]bool)
	}

	// The index list only contains indexes managed by the current node.
	// When the allocation of shards changes, we need to close the indexes that are not managed by this node
	// and delete them from the index list
	curIndexList := ZINC_INDEX_LIST.List()
	removed := make([]*Index, 0)
	for _, index := range curIndexList {
		sum := md5.Sum([]byte(index.GetName()))
		str := hex.EncodeToString(sum[:])
		partition := str[:2]

		// the index not assign to us, we should close it.
		if _, ok := unassignMap[partition]; ok {
			log.Debug().Msgf("unload index: %s not assign to current node", index.GetName())
			ZINC_INDEX_LIST.invalidated[index.GetName()] = false
			if index, ok := ZINC_INDEX_LIST.Remove(index.GetName()); ok {
				removed = append(removed, index)
			}
		}
	}
	ZINC_INDEX_LIST.lifecycleLock.Unlock()
	for _, index := range removed {
		if err := index.Close(); err != nil {
			return err
		}
	}
	return nil
}
