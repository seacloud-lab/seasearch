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
	"time"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
)

var (
	indexUpdateCloser = z.NewCloser(1)
	version           []byte
)

func InitIndexList() {
	// check version
	version, _ = metadata.KV.Get("version")
	if version == nil {
		// version have version from v0.2.5
		// so if no version, it should be <= v0.2.4
		version = []byte("v0.2.4")
	}

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

// LazyCloseSecondIndexShardWriters
// We close all unused indexSecondShard writers in a delayed manner,
// which is to ensure that the finishing work inside the writer can proceed normally,
// and to ensure that the underlying resources can be released.
func LazyCloseSecondIndexShardWriters() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		secondShardList := make([]*tempSecondShd, 0)
		for _, idx := range IndexMgr.GetCached() {
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
	curIndexList := IndexMgr.GetCached()
	removed := make([]*Index, 0)
	for _, index := range curIndexList {
		sum := md5.Sum([]byte(index.GetName()))
		str := hex.EncodeToString(sum[:])
		partition := str[:2]

		// the index not assign to us, we should close it.
		if _, ok := unassignMap[partition]; ok {
			log.Debug().Msgf("unload index: %s not assign to current node", index.GetName())
			if index, ok := IndexMgr.DropCache(index.GetName()); ok {
				removed = append(removed, index)
			}
		}
	}
	for _, index := range removed {
		if err := index.Close(); err != nil {
			return err
		}
	}
	return nil
}
