package main

import (
	"fmt"
	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"math/rand"
	"time"
)

var (
	managerCloser = z.NewCloser(1)

	curAssignMap = make(map[string]int)
)

func InitClusterManger() {
	cluster.InitEtcd(conf.Cluster.EtcdPrefix, conf.Cluster.EtcdEndpoints)

	go assign()
}

func ShutDownClusterManager() {
	managerCloser.SignalAndWait()
	cluster.CloseEtcd()
}

func assign() {
	assignMap, err := cluster.ListAssigns(managerCloser.Ctx())
	if err != nil {
		log.Fatal().Msgf("list assign error: %s", err)
	}
	curAssignMap = assignMap

	tick := time.NewTicker(10 * time.Second)
	defer managerCloser.Done()

	for {
		select {
		case <-managerCloser.HasBeenClosed():
			return
		case <-tick.C:
			if err := execAssign(); err != nil {
				log.Error().Msgf("assign to nodes error: %s", err)
			}
		}
	}
}

func execAssign() error {
	curNodeIds, err := cluster.ListAvailableNodes(managerCloser.Ctx())
	if err != nil {
		return fmt.Errorf("assign error: %w", err)
	}
	if len(curNodeIds) == 0 {
		return nil
	}

	updateAssigns := distribute(curNodeIds)
	if len(updateAssigns) <= 0 {
		return nil
	}

	if err := cluster.PutAssigns(managerCloser.Ctx(), updateAssigns); err != nil {
		return fmt.Errorf("update assigns error: %w", err)
	}
	for partition, nodeId := range updateAssigns {
		curAssignMap[partition] = nodeId
	}
	return nil
}

func distribute(curNodeIds []int) map[string]int {
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
