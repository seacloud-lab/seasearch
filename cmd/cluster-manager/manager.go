package main

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"slices"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/haiwen/goutils/clusterkit"
	"github.com/rs/zerolog/log"
)

var (
	closer = z.NewCloser(1)
)

func InitClusterManger() {
	err := clusterkit.Open(conf.Cluster.EtcdEndpoints, conf.Cluster.EtcdPrefix)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open clusterkit")
	}
	go watchCluster()
}

func ShutDownClusterManager() {
	closer.SignalAndWait()
	clusterkit.Close()
}

func watchCluster() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("watchCluster crashed: %v\n%v", err, debug.Stack())
		}
		closer.Done()
	}()

	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case <-time.After(time.Second * 15):
			// do nothing
		}

		err := updateAssigns()
		if err != nil {
			log.Warn().Err(err).Msg("failed to update assigns")
		}
	}
}

// updateAssigns rebalances the 256 two-hex-digit prefixes across the currently alive nodes.
func updateAssigns() error {
	// fetch heartbeats and cluster nodes
	hbs, err := clusterkit.ListHeartbeats(closer.Ctx())
	if err != nil {
		return fmt.Errorf("failed to list heartbeats: %w", err)
	}
	cluster, _, err := clusterkit.GetClusterNodes(closer.Ctx())
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes: %w", err)
	}

	// filter deleted nodes
	var alive []int
	for _, hb := range hbs {
		exist := slices.ContainsFunc(cluster.Nodes,
			func(node clusterkit.ClusterNode) bool { return node.ID == hb.NodeID },
		)
		if !exist {
			continue
		}
		alive = append(alive, hb.NodeID)
	}
	if len(alive) == 0 {
		// nothing to do when no alive nodes
		return nil
	}

	// fetch current assignments
	assigns, _, err := clusterkit.ListAssigns(closer.Ctx())
	if err != nil {
		return fmt.Errorf("failed to list assigns: %w", err)
	}

	hash := make(map[string]int)
	for _, assign := range assigns {
		hash[assign.Prefix] = assign.NodeID
	}
	// Node IDs should be sorted to keep the distribute result steady.
	slices.Sort(alive)
	hash = distribute(alive, hash)

	if len(hash) > 0 {
		var updates []clusterkit.Assign
		for k, v := range hash {
			updates = append(updates, clusterkit.Assign{
				Prefix: k, NodeID: v,
			})
		}
		err := clusterkit.SetAssigns(updates)
		if err != nil {
			return fmt.Errorf("failed to set assigns: %w", err)
		}
		log.Info().Msg("cluster assigns updated")
	}

	return nil
}

func distribute(curNodeIds []int, curAssignMap map[string]int) map[string]int {
	fullAssigns := make(map[string]struct{})
	for i := 0; i < 1<<8; i++ {
		fullAssigns[fmt.Sprintf("%02x", i)] = struct{}{}
	}

	curNodeMap := make(map[int]struct{})
	for _, node := range curNodeIds {
		curNodeMap[node] = struct{}{}
	}

	// alive nodeId -> assign list
	curNodeAssignMap := make(map[int][]string)
	for assign, nodeId := range curAssignMap {
		if _, ok := curNodeMap[nodeId]; !ok {
			// not alive
			continue
		}
		if list, ok := curNodeAssignMap[nodeId]; ok {
			curNodeAssignMap[nodeId] = append(list, assign)
		} else {
			list = []string{assign}
			curNodeAssignMap[nodeId] = list
		}
	}

	pool := make([]string, 0)
	for id := range fullAssigns {
		// not assign to any node
		node, ok := curAssignMap[id]
		if !ok {
			pool = append(pool, id)
			continue
		}
		// the node is not available
		if _, ok := curNodeMap[node]; !ok {
			pool = append(pool, id)
		}
	}

	average := len(fullAssigns) / len(curNodeIds)
	remainder := len(fullAssigns) % len(curNodeIds)
	i := 0
	NodeAssignResult := make(map[int][]string)

	// make all nodes not exceed the average
	for _, nodeId := range curNodeIds {
		n := average
		if i < remainder {
			n++
		}
		i++
		list := curNodeAssignMap[nodeId]
		if len(list) > n {
			pool = append(pool, list[n:]...)
			curNodeAssignMap[nodeId] = list[:n]
		}
	}

	// there is no need to distribute
	if len(pool) == 0 {
		return nil
	}

	rand.Shuffle(len(pool), func(i, j int) {
		pool[i], pool[j] = pool[j], pool[i]
	})

	i = 0
	for _, nodeId := range curNodeIds {
		n := average
		if i < remainder {
			n++
		}
		i++

		list := curNodeAssignMap[nodeId]
		if n == len(list) {
			continue
		}
		// n > len (list) has been guaranteed before
		m := n - len(list)
		NodeAssignResult[nodeId] = append(list, pool[:m]...)
		pool = pool[m:]
	}

	updatedAssigns := make(map[string]int)
	for node, partitions := range NodeAssignResult {
		for _, partition := range partitions {
			if nodeId, ok := curAssignMap[partition]; ok {
				if nodeId != node {
					updatedAssigns[partition] = node
				}
			} else {
				updatedAssigns[partition] = node
			}
		}
	}

	return updatedAssigns
}
