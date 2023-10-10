package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"

	"sync"
	"time"
)

var (
	// nodeId -> url
	nodeMap *NodeMap
	// partition -> node id
	assignMap *AssignMap
	closer    = z.NewCloser(2)
)

func StartProxy() {

	cluster.InitEtcd(config.Global.Etcd.Prefix, config.Global.Etcd.Endpoints)
	nodeMap = &NodeMap{
		mp: make(map[int]string),
	}
	assignMap = &AssignMap{
		mp: make(map[string]int),
	}

	infos, err := cluster.GetClusterInfo(closer.Ctx())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init proxy")
	}
	updateUrl(infos)

	assigns, err := cluster.ListAssigns(closer.Ctx())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init proxy")
	}
	initAssigns(assigns)

	go watchClusterInfo()

	go watchAssign()
}

func ShutDownProxy() {
	closer.SignalAndWait()
}

type NodeMap struct {
	mp       map[int]string
	nodeList []string
	// used for round-robin
	c     int64
	mutex sync.RWMutex
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

func getAssignNodeByIndex(indexName string) (int, bool) {
	sum := sha256.Sum256([]byte(indexName))
	str := hex.EncodeToString(sum[:])
	assign := str[:2]

	assignMap.mutex.RLock()
	defer assignMap.mutex.RUnlock()

	id, ok := assignMap.mp[assign]
	return id, ok
}

func watchClusterInfo() {
	defer closer.Done()

	errCount := 0
	watch := func(ch <-chan *cluster.ClusterInfoEvent) error {
		for {
			select {
			case <-closer.HasBeenClosed():
				return nil
			case event, ok := <-ch:
				if !ok {
					return fmt.Errorf("watch cluster info goroutine chrashed")
				}
				if event.Err != nil {
					return event.Err
				}
				if !event.Valid {
					return fmt.Errorf("invalid assign event")
				}
				errCount = 0
				if event.ItemUpdated {
					updateUrl(event.Info)
				} else {
					log.Warn().Msg("cluster info was removed")
				}
			}
		}
	}

	for {
		if errCount == 3 {
			log.Fatal().Msg("retry to get assigns too many times")
		}
		ch := cluster.WatchClusterInfo(closer.Ctx())
		err := watch(ch)
		if err != nil {
			errCount++
			log.Error().Err(err).Msg("watch cluster info error, retrying")

			continue
		}
		return
	}
}

func updateUrl(infos []cluster.NodeInfo) {
	nodeMap.mutex.Lock()
	defer nodeMap.mutex.Unlock()

	nodeMap.mp = make(map[int]string)
	nodeMap.nodeList = make([]string, len(infos))

	for i, info := range infos {
		nodeMap.mp[info.NodeId] = info.Address
		nodeMap.nodeList[i] = info.Address
	}
}

type AssignMap struct {
	mp    map[string]int
	mutex sync.RWMutex
}

func watchAssign() {
	defer closer.Done()

	errCount := 0
	watch := func(ch <-chan *cluster.AssignEvent) error {
		for {
			select {
			case <-closer.HasBeenClosed():
				return nil
			case event, ok := <-ch:
				if !ok {
					return fmt.Errorf("watch goroutine chrashed")
				}
				if event.Err != nil {
					return event.Err
				}
				if !event.Valid {
					return fmt.Errorf("invalid assign event")
				}
				errCount = 0
				if event.ItemUpdated {
					setAssign(event.Assign.Partition, event.Assign.NodeId)
				} else {
					removeAssign(event.Assign.Partition)
				}
			}
		}
	}

	for {
		if errCount == 3 {
			log.Fatal().Msg("retry to get assigns too many times")
		}
		ch := cluster.WatchAssigns(closer.Ctx())
		err := watch(ch)
		if err != nil {
			errCount++
			log.Error().Err(err).Msg("watch assign error, retrying")
			time.Sleep(1 * time.Second)
			continue
		}
		return
	}
}

func initAssigns(assigns map[string]int) {
	assignMap.mutex.Lock()
	defer assignMap.mutex.Unlock()

	for partition, nodeId := range assigns {
		assignMap.mp[partition] = nodeId
	}
}

func setAssign(partition string, nodeId int) {
	assignMap.mutex.Lock()
	defer assignMap.mutex.Unlock()
	assignMap.mp[partition] = nodeId
}

func removeAssign(partition string) {
	assignMap.mutex.Lock()
	defer assignMap.mutex.Unlock()
	delete(assignMap.mp, partition)
}
