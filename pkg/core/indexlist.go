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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"golang.org/x/sync/errgroup"
	"sort"
	"sync"
)

var (
	ZINC_INDEX_LIST   IndexList
	indexUpdateCloser = z.NewCloser(1)
	version           []byte
)

type IndexList struct {
	Indexes map[string]*Index
	lock    sync.RWMutex
}

func init() {
	// check version
	version, _ = metadata.KV.Get("version")
	if version == nil {
		// version have version from v0.2.5
		// so if no version, it should be <= v0.2.4
		version = []byte("v0.2.4")
	}

	// start loading index
	ZINC_INDEX_LIST.Indexes = make(map[string]*Index)

	// in cluster mode, we load metadata later when assigns ready
	if config.Global.ServerMode != config.ServerModeCluster {
		err := LoadZincIndexesFromMetadata(string(version))
		if err != nil {
			log.Fatal().Err(err).Msg("Error loading index")
		}
	}

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
}

func (t *IndexList) Add(index *Index) {
	t.lock.Lock()
	t.Indexes[index.GetName()] = index
	t.lock.Unlock()
}

func (t *IndexList) Get(name string) (*Index, bool) {
	t.lock.RLock()
	idx, ok := t.Indexes[name]
	t.lock.RUnlock()
	return idx, ok
}

func (t *IndexList) GetOrCreate(name, storageType string, shardNum int64) (*Index, bool, error) {
	t.lock.RLock()
	idx, ok := t.Indexes[name]
	t.lock.RUnlock()
	if ok {
		return idx, true, nil
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	// maybe someone else created it while we were waiting for the lock
	idx, ok = t.Indexes[name]
	if ok {
		return idx, true, nil
	}
	// okay, let's create new index
	idx, err := NewIndex(name, storageType, shardNum)
	if err != nil {
		return nil, false, err
	}
	// check index
	checkIndex(idx)
	if err = storeIndex(idx); err != nil {
		return nil, false, err
	}
	// cache it
	t.Indexes[idx.GetName()] = idx
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
	t.lock.RLock()
	indexes := make(map[string]*Index, len(t.Indexes))
	for _, index := range t.Indexes {
		indexes[index.ref.Name] = index
	}
	t.lock.RUnlock()
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

// GC auto close unused indexes what inactive for a long time (10m)
func (t *IndexList) GC() error {
	return nil // TODO: implement GC
}

func InitIndexList() {
	if config.Global.ServerMode != config.ServerModeCluster {
		return
	}
	err := LoadZincIndexesFromMetadata(string(version))
	if err != nil {
		log.Fatal().Err(err).Msg("init assign error")
	}
	go watchIndexUpdate()
}

func CloseIndexList() {
	if config.Global.ServerMode != config.ServerModeCluster {
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
		case removeMap := <-cluster.AssignChan:
			err := updateIndexList(removeMap)
			if err != nil {
				log.Error().Err(err).Msg("cannot update memory index list")
			}
		}
	}
}

func updateIndexList(removeMap map[string]struct{}) error {
	// The index list only contains indexes managed by the current node.
	// When the allocation of shards changes, we need to close the indexes that are not managed by this node
	// and delete them from the index list
	curIndexList := ZINC_INDEX_LIST.List()
	for _, index := range curIndexList {
		sum := sha256.Sum256([]byte(index.GetName()))
		str := hex.EncodeToString(sum[:])
		partition := str[:2]

		// the index not assign to us, we should close it.
		if _, ok := removeMap[partition]; ok {
			log.Debug().Msgf("unload index: %s not assign to current node", index.GetName())
			ZINC_INDEX_LIST.Delete(index.GetName())
		}
	}
	// When the assign of partition changes, some partition and indexes may be reassigned to this node.
	// We load the indexes of these partitions.
	// Note that in daily operation, the partition assign does not change,
	// the creation and deletion of index are done at the node responsible for the partition,
	// and the index list is automatically updated.
	fullIndexes, err := metadata.Index.List(0, 0)
	if err != nil {
		return err
	}

	for _, index := range fullIndexes {
		// already here
		if _, ok := ZINC_INDEX_LIST.Get(index.Name); ok {
			continue
		}
		if cluster.AssignCheck(index.Name) {
			// the index assign to us, so we load it.
			err := LoadZincIndexFromMetadata(string(version), index)
			if err != nil {
				return fmt.Errorf("reload %s metadata error: %w", index.Name, err)
			}
		}
	}
	return nil
}
