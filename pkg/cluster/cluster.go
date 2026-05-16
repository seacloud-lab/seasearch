package cluster

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/zincsearch/zincsearch/pkg/config"

	"github.com/dgraph-io/ristretto/z"
	"github.com/haiwen/goutils/clusterkit"
	"github.com/rs/zerolog/log"
)

var (
	closer    = z.NewCloser(4)
	assignMap = make(map[string]struct{})
	lock      sync.RWMutex

	// UnassignChan used for notify core update memory index list
	UnassignChan = make(chan map[string]struct{})
	// UserChan used for notify core update user list
	UserChan = make(chan struct{})
	// RoleChan used for notify core update role list
	RoleChan = make(chan struct{})
)

func Init() {
	if !config.Global.Cluster.Enable {
		return
	}

	err := clusterkit.Open(config.Global.Etcd.Endpoints, config.Global.Etcd.Prefix)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open clusterkit")
	}

	go keepHeartbeat()

	go syncAssigns()

	go syncUsers()

	go syncRoles()
}

func Close() {
	if !config.Global.Cluster.Enable {
		return
	}
	closer.SignalAndWait()
	clusterkit.Close()
}

func keepHeartbeat() {
	defer closer.Done()

	for {
		err := clusterkit.KeepHeartbeat(closer.Ctx(), config.Global.Cluster.NodeId)
		if err != nil {
			log.Warn().Err(err).Msg("failed keep heartbeat")
		}

		select {
		case <-closer.HasBeenClosed():
			return
		case <-time.After(time.Second * 15):
			continue
		}
	}
}

func syncAssigns() {
	defer closer.Done()

	for {
		err := watchAssigns()
		if err != nil {
			log.Warn().Err(err).Msg("failed to watch assigns")
		}

		// Reset the cluster information when it goes out of sync.
		lock.Lock()
		assignMap = make(map[string]struct{})
		lock.Unlock()

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

	lock.Lock()

	assignMap = make(map[string]struct{})
	unassignMap := make(map[string]struct{})
	for _, assign := range assigns {
		if assign.NodeID == config.Global.Cluster.NodeId {
			assignMap[assign.Prefix] = struct{}{}
		} else {
			unassignMap[assign.Prefix] = struct{}{}
		}
	}

	lock.Unlock()

	select {
	case <-closer.HasBeenClosed():
	case UnassignChan <- unassignMap:
	}
	return rev, nil
}

func AssignCheck(indexName string) bool {
	if !config.Global.Cluster.Enable {
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

func syncUsers() {
	defer closer.Done()

	for {
		it := WatchUsers(closer.Ctx())
		for range it.Range() {
			select {
			case <-closer.HasBeenClosed():
				return
			case UserChan <- struct{}{}:
			}
		}
		if it.Error() != nil {
			log.Warn().Err(it.Error()).Msg("failed to watch users")
		}

		select {
		case <-closer.HasBeenClosed():
			return
		case <-time.After(time.Second * 15):
			continue
		}
	}
}

func syncRoles() {
	defer closer.Done()

	for {
		it := WatchRoles(closer.Ctx())
		for range it.Range() {
			select {
			case <-closer.HasBeenClosed():
				return
			case RoleChan <- struct{}{}:
			}
		}
		if it.Error() != nil {
			log.Warn().Err(it.Error()).Msg("failed to watch roles")
		}

		select {
		case <-closer.HasBeenClosed():
			return
		case <-time.After(time.Second * 15):
			continue
		}
	}
}
