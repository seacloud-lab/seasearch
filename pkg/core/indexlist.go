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
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/errors"
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
	Indexes  map[string]*Index
	lock     sync.RWMutex
	gcCloser *z.Closer
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
	ZINC_INDEX_LIST.gcCloser = z.NewCloser(1)

	go LazyCloseSecondIndexShardWriters()

	// update version
	if string(version) != meta.Version {
		err := metadata.KV.Set("version", []byte(meta.Version))
		if err != nil {
			log.Error().Err(err).Msg("Error set version")
		}
	}

	aliases, err := metadata.Alias.Get()
	if err != nil {
		log.Fatal().Err(err).Msg("Error loading alias")
	}
	ZINC_INDEX_ALIAS_LIST.Aliases = aliases

	if config.Global.Cluster.Enable {
		go watchIndexUpdate()
	}
}

func (t *IndexList) Add(index *Index) {
	t.lock.Lock()
	t.Indexes[index.GetName()] = index
	index.atime = time.Now().Unix()
	t.lock.Unlock()
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
	t.lock.Lock()
	defer t.lock.Unlock()

	idx, ok := t.Indexes[name]
	if ok {
		atomic.StoreInt64(&idx.atime, time.Now().Unix())
		return idx, true, nil
	}

	index, err := GetZincIndexFromMetadata(name)
	if errors.Is(err, errors.ErrKeyNotFound) {
		// continue
	} else if err != nil {
		return nil, false, err
	} else {
		t.Indexes[index.GetName()] = index
		index.atime = time.Now().Unix()
		return index, true, nil
	}

	// maybe someone else created it while we were waiting for the lock
	idx, ok = t.Indexes[name]
	if ok {
		return idx, true, nil
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
	// cache it
	t.Indexes[idx.GetName()] = idx
	idx.atime = time.Now().Unix()
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

func (t *IndexList) ListCached() []*Index {
	t.lock.RLock()
	defer t.lock.RUnlock()
	indexes := slices.Collect(maps.Values(t.Indexes))
	return indexes
}

func (t *IndexList) ListName() []string {
	names, err := metadata.Index.ListNames(0, 0)
	if err != nil {
		log.Error().Err(err).Msg("failed to list names in metadata")
		return nil
	}
	slices.Sort(names)
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

// LazyCloseSecondIndexShardWriters
// We close all unused indexSecondShard writers in a delayed manner,
// which is to ensure that the finishing work inside the writer can proceed normally,
// and to ensure that the underlying resources can be released.
func LazyCloseSecondIndexShardWriters() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		secondShardList := make([]*tempSecondShd, 0)
		for _, idx := range ZINC_INDEX_LIST.ListCached() {
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
	// The index list only contains indexes managed by the current node.
	// When the allocation of shards changes, we need to close the indexes that are not managed by this node
	// and delete them from the index list
	indexNames := ZINC_INDEX_LIST.ListName()
	for _, name := range indexNames {
		sum := md5.Sum([]byte(name))
		str := hex.EncodeToString(sum[:])
		partition := str[:2]

		// the index not assign to us, we should close it.
		if _, ok := unassignMap[partition]; ok {
			log.Debug().Msgf("unload index: %s not assign to current node", name)
			ZINC_INDEX_LIST.Delete(name)
		}
	}
	return nil
}
