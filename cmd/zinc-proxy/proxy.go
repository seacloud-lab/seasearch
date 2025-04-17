package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/cluster"
)

var (
	// nodeId -> url
	nodeMap *NodeMap
	// partition -> node id
	assignMap *AssignMap
	closer    = z.NewCloser(1)
)

type AssignMap struct {
	mp    map[string]int
	mutex sync.RWMutex
}

type NodeMap struct {
	mp       map[int]string
	nodeList []nodeInfo
	mutex    sync.RWMutex
}

type nodeInfo struct {
	id   int
	addr string
}

func StartProxy() {
	cluster.InitEtcd(conf.Etcd.Prefix, conf.Etcd.Endpoints)
	nodeMap = &NodeMap{
		mp: make(map[int]string),
	}
	assignMap = &AssignMap{
		mp: make(map[string]int),
	}

	go syncClusterInfo()
}

func syncClusterInfo() {
	defer closer.Done()

	defer func() {
		if err := recover(); err != nil {
			log.Printf("sync cluster info goroutine crashed: %v\n%s", err, debug.Stack())
		}
	}()

	for {
		err := syncCluster()
		if err != nil {
			log.Err(err).Msg("failed to sync cluster info: ")
			log.Info().Msg("clearing cluster map")
			updateClusterInfo(nil)
		}

		time.Sleep(time.Minute)
	}
}

func syncCluster() error {
	infos, err := cluster.GetClusterInfo(closer.Ctx())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init proxy")
	}
	updateClusterInfo(infos)

	assigns, err := cluster.ListAssigns(closer.Ctx())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init proxy")
	}

	assignMap.mutex.Lock()
	for partition, nodeId := range assigns {
		assignMap.mp[partition] = nodeId
	}
	assignMap.mutex.Unlock()

	assignCh := cluster.WatchAssigns(closer.Ctx())
	nodeCh := cluster.WatchClusterNodeInfo(closer.Ctx())

	for {
		select {
		case event, ok := <-assignCh:
			if !ok {
				return nil
			}
			if event.Err != nil {
				return event.Err
			}
			if !event.Valid {
				return fmt.Errorf("invalid assign event")
			}

			log.Debug().Msgf("update assign")
			if event.ItemUpdated {
				assignMap.mutex.Lock()
				assignMap.mp[event.Assign.Partition] = event.Assign.NodeId
				assignMap.mutex.Unlock()
			} else {
				assignMap.mutex.Lock()
				delete(assignMap.mp, event.Assign.Partition)
				assignMap.mutex.Unlock()
			}

		case event, ok := <-nodeCh:
			if !ok {
				return nil
			}
			if event.Err != nil {
				return event.Err
			}
			if !event.Valid {
				return fmt.Errorf("invalid assign event")
			}
			log.Debug().Msgf("update nodes")

			if event.ItemUpdated {
				updateClusterInfo(event.Info)
			} else {
				updateClusterInfo(nil)
				log.Warn().Msg("cluster node was removed")
			}
		}
	}
}

func ShutDownProxy() {
	closer.SignalAndWait()
}

func GetAddrByIndex(indexName string) (string, error) {
	nodeId, ok := getAssignNodeByIndex(indexName)
	if !ok {
		return "", fmt.Errorf("the index: %s is not assigned to any node", indexName)
	}

	nodeMap.mutex.RLock()
	defer nodeMap.mutex.RUnlock()

	addr, ok := nodeMap.mp[nodeId]
	if !ok {
		return "", fmt.Errorf("the node %d for index: %s is not registered in etcd", nodeId, indexName)
	}

	return addr, nil
}

func GetNodeList() []nodeInfo {
	nodeMap.mutex.RLock()
	defer nodeMap.mutex.RUnlock()
	res := make([]nodeInfo, 0, len(nodeMap.nodeList))
	for _, item := range nodeMap.nodeList {
		res = append(res, nodeInfo{
			id:   item.id,
			addr: item.addr,
		})
	}
	return res
}

func getAssignNodeByIndex(indexName string) (int, bool) {
	sum := md5.Sum([]byte(indexName))
	str := hex.EncodeToString(sum[:])
	assign := str[:2]

	assignMap.mutex.RLock()
	defer assignMap.mutex.RUnlock()

	id, ok := assignMap.mp[assign]
	return id, ok
}

func updateClusterInfo(infos []cluster.NodeInfo) {
	nodeMap.mutex.Lock()
	defer nodeMap.mutex.Unlock()

	nodeMap.mp = make(map[int]string)
	nodeMap.nodeList = make([]nodeInfo, len(infos))

	for i, info := range infos {
		nodeMap.mp[info.NodeId] = info.Address
		nodeMap.nodeList[i] = nodeInfo{
			id:   info.NodeId,
			addr: info.Address,
		}
	}
	sort.Slice(nodeMap.nodeList, func(i, j int) bool {
		return nodeMap.nodeList[i].id < nodeMap.nodeList[j].id
	})
}
