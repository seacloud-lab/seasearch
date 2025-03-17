package cluster

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/config"
	client "go.etcd.io/etcd/client/v3"
)

var (
	closer                = z.NewCloser(4)
	assignMap             = make(map[string]struct{})
	lastAssignProcessTime time.Time
	lock                  sync.RWMutex

	// AssignChan used for notify core update memory index list
	AssignChan = make(chan map[string]struct{})

	// RoleChan used for notify core update role list
	RoleChan = make(chan struct{})
	// UserChan  used for notify core update user list
	UserChan = make(chan struct{})
)

func Init() {
	if config.Global.ServerMode != config.ServerModeCluster {
		return
	}

	InitEtcd(config.Global.Etcd.Prefix, config.Global.Etcd.Endpoints)
	if err := InitAssigns(); err != nil {
		log.Fatal().Msgf("init cluster error: %s", err)
	}

	go keepHeartBeat()

	go watchAssign()

	go watchUser()

	go watchRole()
}

func Close() {
	if config.Global.ServerMode != config.ServerModeCluster {
		return
	}
	closer.SignalAndWait()
	CloseEtcd()
}

func InitAssigns() error {
	assigns, err := ListAssigns(closer.Ctx())
	if err != nil {
		return fmt.Errorf("init assigns error: %s", err)
	}
	lock.Lock()
	defer lock.Unlock()
	for partition, nodeId := range assigns {
		if nodeId == config.Global.Cluster.NodeId {
			assignMap[partition] = struct{}{}
		}
	}
	return nil
}

func keepHeartBeat() {
	defer closer.Done()

	key := fmt.Sprintf("%s/cluster/hb/%d", config.Global.Etcd.Prefix, config.Global.Cluster.NodeId)
	ls := client.NewLease(cli)

	var curLeaseId client.LeaseID = 0
	ticker := time.NewTicker(60 * time.Second)

	first := make(chan struct{}, 1)
	first <- struct{}{}

	var err error
	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case <-first:
			curLeaseId, err = SendHeartBeat(key, curLeaseId, ls)
			if err != nil {
				log.Fatal().Err(err).Msg("cannot send heart beat to etcd")
			}
		case <-ticker.C:
			curLeaseId, err = SendHeartBeat(key, curLeaseId, ls)
			if err != nil {
				log.Warn().Err(err).Msg("cannot send heart beat to etcd")
			}
		}
	}
}

// DiffAssigns
// update current assigns with input map and return should be closed assigns
// we don't return addMap because we have no way to only load the specified index according to partition id.
func DiffAssigns(inputAssigns map[string]int) (removeMap map[string]struct{}) {
	removeMap = make(map[string]struct{})

	lock.Lock()
	for partition, nodeId := range inputAssigns {
		// assign to us
		if nodeId == config.Global.Cluster.NodeId {
			assignMap[partition] = struct{}{}
		} else {
			// we used to have it, but not anymore
			if _, ok := assignMap[partition]; ok {
				delete(assignMap, partition)
				removeMap[partition] = struct{}{}
			}
		}
	}
	// not present in inputAssigns, they don't belong to anyone
	for partition := range assignMap {
		if _, ok := inputAssigns[partition]; !ok {
			delete(assignMap, partition)
			removeMap[partition] = struct{}{}
		}
	}
	lock.Unlock()

	return
}

func ClearAssign() {
	lock.Lock()
	defer lock.Unlock()
	assignMap = make(map[string]struct{})
}

func AssignCheck(indexName string) bool {
	if config.Global.ServerMode != config.ServerModeCluster {
		return true
	}

	sum := md5.Sum([]byte(indexName))
	str := hex.EncodeToString(sum[:])
	assign := str[:2]

	lock.RLock()
	defer lock.RUnlock()
	_, ok := assignMap[assign]
	return ok
}

func watchAssign() {
	defer closer.Done()

	watch := func(ch <-chan *AssignEvent) error {
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
				// Assignments in etcd are updated in a transaction.
				// So the events should usually arrive at around the same time.
				// We only need to update our internal assignment cache when the first event arrives.
				// So we set a 10s buffer to skip the following events.
				if time.Since(lastAssignProcessTime) < 10*time.Second {
					// ignore
					continue
				}
				lastAssignProcessTime = time.Now()
				assigns, err := ListAssigns(closer.Ctx())
				if err != nil {
					return fmt.Errorf("update assigns error: %s", err)
				}
				removeMap := DiffAssigns(assigns)
				AssignChan <- removeMap
			}
		}
	}

	for {
		ch := WatchAssigns(closer.Ctx())
		err := watch(ch)
		if err != nil {
			log.Err(err).Msg("watch assign error, retrying")
			ClearAssign()
			time.Sleep(1 * time.Minute)
			if err = InitAssigns(); err != nil {
				log.Err(err).Msg("get assigns error")
			}
			continue
		}
		return
	}
}

func watchUser() {
	defer closer.Done()
	ch := WatchUserInfo(closer.Ctx())
	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case ev, ok := <-ch:
			if !ok || ev.Err != nil {
				time.Sleep(1 * time.Minute)
				ch = WatchUserInfo(closer.Ctx())
				continue
			}
			UserChan <- struct{}{}
		}
	}
}

func watchRole() {
	defer closer.Done()
	ch := WatchRoleInfo(closer.Ctx())
	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case ev, ok := <-ch:
			if !ok || ev.Err != nil {
				time.Sleep(1 * time.Minute)
				ch = WatchRoleInfo(closer.Ctx())
				continue
			}
			RoleChan <- struct{}{}
		}
	}
}
