package main

import (
	"cmp"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/haiwen/goutils/clusterkit"
	"github.com/rs/zerolog/log"
)

var (
	closer *z.Closer
	// nodeId -> url
	nodeMap *NodeMap
	// partition -> node id
	assignMap *AssignMap
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
	closer = z.NewCloser(0)
	nodeMap = &NodeMap{
		mp: make(map[int]string),
	}
	assignMap = &AssignMap{
		mp: make(map[string]int),
	}
	err := clusterkit.Open(conf.Etcd.Endpoints, conf.Etcd.Prefix)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open clusterkit")
	}

	closer.AddRunning(2)
	go syncClusterNodes()
	go syncAssigns()
}

func ShutDownProxy() {
	closer.SignalAndWait()
	clusterkit.Close()
}

func syncClusterNodes() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("syncClusterNodes crashed: %v\n%v", err, debug.Stack())
		}
		closer.Done()
	}()

	for {
		err := watchClusterNodes()
		if err != nil {
			log.Warn().Err(err).Msg("failed to watch cluster nodes")
		}

		select {
		case <-closer.HasBeenClosed():
			return
		case <-time.After(time.Second * 15):
			continue
		}
	}
}

func watchClusterNodes() error {
	rev, err := updateNodeMp()
	if err != nil {
		return err
	}
	it := clusterkit.WatchClusterNodes(closer.Ctx(), rev)
	for range it.Range() {
		_, err = updateNodeMp()
		if err != nil {
			return err
		}
	}
	return it.Error()
}

func updateNodeMp() (int64, error) {
	nodes, rev, err := clusterkit.GetClusterNodes(closer.Ctx())
	if err != nil {
		return 0, fmt.Errorf("failed to get cluster nodes: %w", err)
	}

	nodeMap.mutex.Lock()
	defer nodeMap.mutex.Unlock()

	nodeMap.mp = make(map[int]string)
	nodeMap.nodeList = nil
	for _, node := range nodes.Nodes {
		nodeMap.mp[node.ID] = node.URL
		nodeMap.nodeList = append(nodeMap.nodeList, nodeInfo{
			id:   node.ID,
			addr: node.URL,
		})
	}
	slices.SortFunc(nodeMap.nodeList, func(a, b nodeInfo) int {
		return cmp.Compare(a.id, b.id)
	})

	return rev, nil
}

func syncAssigns() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("syncAssigns crashed: %v\n%v", err, debug.Stack())
		}
		closer.Done()
	}()

	for {
		err := watchAssigns()
		if err != nil {
			log.Warn().Err(err).Msg("failed to watch assigns")
		}

		// Reset the cluster information when it goes out of sync.
		assignMap.mutex.Lock()
		assignMap.mp = make(map[string]int)
		assignMap.mutex.Unlock()

		select {
		case <-closer.HasBeenClosed():
			return
		case <-time.After(time.Second * 15):
			continue
		}
	}
}

func watchAssigns() error {
	rev, err := updateAssigns()
	if err != nil {
		return err
	}
	it := clusterkit.WatchAssigns(closer.Ctx(), rev)
	for range it.Range() {
		_, err = updateAssigns()
		if err != nil {
			return err
		}
	}
	return it.Error()
}

func updateAssigns() (int64, error) {
	assigns, rev, err := clusterkit.ListAssigns(closer.Ctx())
	if err != nil {
		return 0, fmt.Errorf("failed to list assigns: %w", err)
	}

	assignMap.mutex.Lock()
	defer assignMap.mutex.Unlock()

	assignMap.mp = make(map[string]int)
	for _, assign := range assigns {
		assignMap.mp[assign.Prefix] = assign.NodeID
	}

	return rev, nil
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
