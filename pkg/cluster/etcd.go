package cluster

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
	"go.uber.org/zap"

	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	client "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	DefaultTimeout = time.Second * 10
)

var (
	cli    *client.Client
	prefix string

	// we cannot know whether disconnected with etcd by go-etcd watcher,
	// so we establish a session so that we can know when there is a problem with the network of etcd.
	curSession   *concurrency.Session
	sessionMutex sync.Mutex

	// the NewSession will be blocked and won't return if the connection is unavailable,
	// so we use a custom context to control the timeout of NewSession.
	sessionCtx    context.Context
	sessionCancel context.CancelFunc
	etcdCloser    = z.NewCloser(1)
)

func InitEtcd(p string, endpoints []string) error {
	var err error
	conf := logutil.DefaultZapLoggerConfig
	conf.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)

	cli, err = client.New(client.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		LogConfig:   &conf,
	})
	if err != nil {
		log.Fatal().Err(err).Msgf("init etcd for failed")
	}
	prefix = p

	// We use timer to control timeout cancellation instead of
	// ContextWithTimeout because the latter will exit the normal session after timeout.
	sessionCtx, sessionCancel = context.WithCancel(context.Background())
	timer := time.AfterFunc(DefaultTimeout, func() { sessionCancel() })
	curSession, err = concurrency.NewSession(
		cli, concurrency.WithTTL(3), concurrency.WithContext(sessionCtx),
	)
	timer.Stop()
	if err != nil {
		sessionCancel()
		if errors.Is(err, context.Canceled) {
			return fmt.Errorf("failed to establish a session to the etcd: connection timeout: %w", err)
		}
		return fmt.Errorf("failed to establish a session to the etcd: %w", err)
	}
	go maintainSession()
	return nil
}

// maintainSession
// when the network disconnected, the old session will be invalid,
// we need to create a new session.
func maintainSession() {
	defer etcdCloser.Done()
	for {
		select {
		case <-etcdCloser.HasBeenClosed():
			sessionCancel()
			return
		case <-curSession.Done():
			// cancel current context
			sessionCancel()
			log.Info().Msg("trying to reconnect etcd")
			_ = curSession.Close()

			// crate new ctx with cancel
			sessionCtx, sessionCancel = context.WithCancel(context.Background())
			timer := time.AfterFunc(DefaultTimeout, func() { sessionCancel() })
			newSession, err := concurrency.NewSession(
				cli, concurrency.WithTTL(3), concurrency.WithContext(sessionCtx),
			)
			timer.Stop()

			if err != nil {
				sessionCancel()
				if errors.Is(err, context.Canceled) {
					log.Err(err).Msg("failed to establish a session to the etcd: connection timeout: ")
				} else {
					log.Err(err).Msg("failed to establish a session to the etcd: ")
				}
				time.Sleep(3 * time.Second)
				continue
			}
			log.Info().Msg("reconnect etcd success")
			sessionMutex.Lock()
			curSession = newSession
			sessionMutex.Unlock()
		}
	}
}

func CloseEtcd() {
	etcdCloser.SignalAndWait()
	err := cli.Close()
	if err != nil {
		log.Warn().Err(err).Msgf("close etcd client error")
	}
}

func ListAvailableNodes(ctx context.Context) ([]int, error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	prefix := fmt.Sprintf("%s/cluster/hb/", prefix)
	rsp, err := cli.Get(ctx, prefix, client.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get %s from etcd: %w", prefix, err)
	}

	var nodes []int
	for _, kv := range rsp.Kvs {
		var id int
		if _, err := fmt.Sscanf(string(kv.Key), prefix+"%d", &id); err != nil {
			return nil, fmt.Errorf("failed to parse id in %s: %w", kv.Key, err)
		}
		nodes = append(nodes, id)
	}
	return nodes, nil
}

type Assign struct {
	Partition string
	NodeId    int
}

func PutAssigns(ctx context.Context, assigns map[string]int) error {
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	var ops []client.Op
	for partition, nodeId := range assigns {
		key := fmt.Sprintf("%s/cluster/assign/%s", prefix, partition)
		val := strconv.Itoa(nodeId)
		ops = append(ops, client.OpPut(key, string(val)))
	}
	// split ops into two transactions
	if len(ops) > 128 {
		_, err := cli.Txn(ctx).Then(ops[:128]...).Commit()
		if err != nil {
			return fmt.Errorf("failed to put assigns to etcd: %w", err)
		}
		_, err = cli.Txn(ctx).Then(ops[128:]...).Commit()
		if err != nil {
			return fmt.Errorf("failed to put assigns to etcd: %w", err)
		}
	} else {
		_, err := cli.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return fmt.Errorf("failed to put assigns to etcd: %w", err)
		}
	}

	return nil
}

func ListAssigns(ctx context.Context) (map[string]int, error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	prefix := fmt.Sprintf("%s/cluster/assign/", prefix)
	rsp, err := cli.Get(ctx, prefix, client.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get %q from etcd: %w", prefix, err)
	}

	var assigns = make(map[string]int)
	for _, kv := range rsp.Kvs {
		var assign Assign
		if _, err := fmt.Sscanf(string(kv.Key), prefix+"%s", &assign.Partition); err != nil {
			return nil, fmt.Errorf("failed to parse shard in %s: %w", kv.Key, err)
		}
		assign.NodeId, err = strconv.Atoi(string(kv.Value))
		if err != nil {
			return nil, fmt.Errorf("failed to parse assign node id in %s: %w", kv.Value, err)
		}

		assigns[assign.Partition] = assign.NodeId
	}
	return assigns, nil
}

type AssignEvent struct {
	Assign
	Valid       bool
	ItemUpdated bool
	Err         error
}

func WatchAssigns(ctx context.Context) <-chan *AssignEvent {
	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(DefaultTimeout, cancel)

	prefix := fmt.Sprintf("%s/cluster/assign/", prefix)
	watcher := client.NewWatcher(cli)
	rspCh := watcher.Watch(client.WithRequireLeader(ctx), prefix, client.WithPrefix())
	timer.Stop()

	ch := make(chan *AssignEvent)
	go watchAssigns(ctx, cancel, watcher, rspCh, ch)

	return ch
}

func watchAssigns(ctx context.Context, cancel context.CancelFunc, watcher client.Watcher, rspCh client.WatchChan, ch chan *AssignEvent) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("watch assigns goroutine crashed: %v\n%s", err, debug.Stack())
		}
		close(ch)
		cancel()
		watcher.Close()
	}()

	prefix := fmt.Sprintf("%s/cluster/assign/", prefix)
	sessionMutex.Lock()
	session := curSession
	sessionMutex.Unlock()
	for {
		select {
		case <-ctx.Done():
			return

		case rsp := <-rspCh:
			if rsp.Err() != nil || len(rsp.Events) == 0 {
				ch <- &AssignEvent{
					Err: rsp.Err(),
				}
				return
			}

			for _, event := range rsp.Events {
				var assign AssignEvent
				if _, err := fmt.Sscanf(string(event.Kv.Key), prefix+"%s", &assign.Partition); err != nil {
					log.Warn().Err(err).Msgf("malformed key in %q", event.Kv.Key)
					continue
				}
				if event.Type == client.EventTypePut {
					assign.ItemUpdated = true
					var err error
					assign.Assign.NodeId, err = strconv.Atoi(string(event.Kv.Value))
					if err != nil {
						log.Warn().Err(err).Msgf("malformed value in %q", event.Kv.Key)
						continue
					}
				}
				assign.Valid = true
				ch <- &assign
			}
		case <-session.Done():
			var event = AssignEvent{}
			event.Err = fmt.Errorf("connection to the etcd has been closed")
			ch <- &event
			return
		}
	}
}

type NodeInfo struct {
	NodeId  int    `json:"node_id"`
	Address string `json:"address"`
}

func PutClusterInfo(ctx context.Context, info []NodeInfo) error {
	key := fmt.Sprintf("%s/cluster/nodes", prefix)
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()
	val, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to put cluster info: %w", err)
	}
	_, err = cli.Put(ctx, key, string(val))
	if err != nil {
		return fmt.Errorf("failed to put clsuter info to etcd: %w", err)
	}
	return nil
}

func GetClusterInfo(ctx context.Context) ([]NodeInfo, error) {
	key := fmt.Sprintf("%s/cluster/nodes", prefix)
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()
	rsp, err := cli.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get %q from etcd: %w", key, err)
	}
	if len(rsp.Kvs) == 0 {
		return nil, errors.ErrKeyNotFound
	}
	var res []NodeInfo
	err = json.Unmarshal(rsp.Kvs[0].Value, &res)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal zinc-cluster-info: %w", err)
	}

	return res, nil
}

type ClusterInfoEvent struct {
	Info        []NodeInfo
	Valid       bool
	ItemUpdated bool
	Err         error
}

func WatchClusterNodeInfo(ctx context.Context) <-chan *ClusterInfoEvent {
	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(DefaultTimeout, cancel)

	key := fmt.Sprintf("%s/cluster/nodes", prefix)
	watcher := client.NewWatcher(cli)
	rspCh := watcher.Watch(client.WithRequireLeader(ctx), key)
	timer.Stop()

	ch := make(chan *ClusterInfoEvent)
	go watchClusterInfo(ctx, cancel, watcher, rspCh, ch)

	return ch
}

func watchClusterInfo(ctx context.Context, cancel context.CancelFunc, watcher client.Watcher, rspCh client.WatchChan, ch chan *ClusterInfoEvent) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("watch clsuter info goroutine crashed: %v\n%s", err, debug.Stack())
		}
		close(ch)
		cancel()
		watcher.Close()
	}()

	sessionMutex.Lock()
	session := curSession
	sessionMutex.Unlock()
	for {
		select {
		case <-ctx.Done():
			return

		case rsp := <-rspCh:
			if rsp.Err() != nil || len(rsp.Events) == 0 {
				ch <- &ClusterInfoEvent{
					Err: rsp.Err(),
				}
				return
			}

			for _, event := range rsp.Events {
				var assign ClusterInfoEvent
				if event.Type == client.EventTypePut {
					assign.ItemUpdated = true
					err := json.Unmarshal(event.Kv.Value, &assign.Info)
					if err != nil {
						log.Warn().Err(err).Msgf("malformed value in %q", event.Kv.Key)
						continue
					}
				}
				assign.Valid = true
				ch <- &assign
			}
		case <-session.Done():
			var event = ClusterInfoEvent{}
			event.Err = fmt.Errorf("connection to the etcd has been closed")
			ch <- &event
			return
		}
	}
}

type IndexEvent struct {
	meta.Index
	Err         error
	Valid       bool
	ItemUpdated bool
}

func SendHeartBeat(key string, curLeaseId client.LeaseID, ls client.Lease) (client.LeaseID, error) {
	timeoutContext, cancel := context.WithTimeout(closer.Ctx(), DefaultTimeout)
	defer cancel()
	if curLeaseId == 0 {
		leaseResp, err := ls.Grant(timeoutContext, 180)
		if err != nil {
			return 0, fmt.Errorf("can not set lease %s to etcd: %w", key, err)
		}
		if _, err := cli.Put(timeoutContext, key, strconv.FormatBool(true), client.WithLease(leaseResp.ID)); err != nil {
			return 0, fmt.Errorf("can not put %s to etcd: %w", key, err)
		}
		return leaseResp.ID, nil
	} else {
		if _, err := ls.KeepAliveOnce(timeoutContext, curLeaseId); err == rpctypes.ErrLeaseNotFound {
			return 0, nil
		} else if err != nil {
			return 0, fmt.Errorf("keep alive error: %w", err)
		}
		return curLeaseId, nil
	}
}

type UserInfoEvent struct {
	meta.User
	Err         error
	Valid       bool
	ItemUpdated bool
}

func WatchUserInfo(ctx context.Context) <-chan *UserInfoEvent {
	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(DefaultTimeout, cancel)

	prefix := fmt.Sprintf("%s/metadata/user/", prefix)
	watcher := client.NewWatcher(cli)
	rspCh := watcher.Watch(client.WithRequireLeader(ctx), prefix, client.WithPrefix())
	timer.Stop()

	ch := make(chan *UserInfoEvent)
	go watchUserInfo(ctx, cancel, watcher, rspCh, ch)

	return ch
}

func watchUserInfo(ctx context.Context, cancel context.CancelFunc, watcher client.Watcher, rspCh client.WatchChan, ch chan *UserInfoEvent) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("watch clsuter info goroutine crashed: %v\n%s", err, debug.Stack())
		}
		close(ch)
		cancel()
		watcher.Close()
	}()

	sessionMutex.Lock()
	session := curSession
	sessionMutex.Unlock()

	for {
		select {
		case <-ctx.Done():
			return

		case rsp := <-rspCh:
			if rsp.Err() != nil {
				ch <- &UserInfoEvent{
					Err: rsp.Err(),
				}
				return
			}

			for _, event := range rsp.Events {
				var userEvent UserInfoEvent
				if event.Type == client.EventTypePut {
					userEvent.ItemUpdated = true
					err := json.Unmarshal(event.Kv.Value, &userEvent.User)
					if err != nil {
						log.Warn().Err(err).Msgf("malformed value in %q", event.Kv.Key)
						continue
					}
				}
				userEvent.Valid = true
				ch <- &userEvent
			}
		case <-session.Done():
			var event = UserInfoEvent{}
			event.Err = fmt.Errorf("connection to the etcd has been closed")
			ch <- &event
			return
		}
	}
}

type RoleEvent struct {
	meta.Role
	Err         error
	Valid       bool
	ItemUpdated bool
}

func WatchRoleInfo(ctx context.Context) <-chan *RoleEvent {
	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(DefaultTimeout, cancel)

	prefix := fmt.Sprintf("%s/metadata/role/", prefix)
	watcher := client.NewWatcher(cli)
	rspCh := watcher.Watch(client.WithRequireLeader(ctx), prefix, client.WithPrefix())
	timer.Stop()

	ch := make(chan *RoleEvent)
	go watchRoleInfo(ctx, cancel, watcher, rspCh, ch)

	return ch
}

func watchRoleInfo(ctx context.Context, cancel context.CancelFunc, watcher client.Watcher, rspCh client.WatchChan, ch chan *RoleEvent) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("watch clsuter info goroutine crashed: %v\n%s", err, debug.Stack())
		}
		close(ch)
		cancel()
		watcher.Close()
	}()

	sessionMutex.Lock()
	session := curSession
	sessionMutex.Unlock()

	for {
		select {
		case <-ctx.Done():
			return

		case rsp := <-rspCh:
			if rsp.Err() != nil {
				ch <- &RoleEvent{
					Err: rsp.Err(),
				}
				return
			}

			for _, event := range rsp.Events {
				var roleEvent RoleEvent
				if event.Type == client.EventTypePut {
					roleEvent.ItemUpdated = true
					err := json.Unmarshal(event.Kv.Value, &roleEvent.Role)
					if err != nil {
						log.Warn().Err(err).Msgf("malformed value in %q", event.Kv.Key)
						continue
					}
				}
				roleEvent.Valid = true
				ch <- &roleEvent
			}
		case <-session.Done():
			var event = RoleEvent{}
			event.Err = fmt.Errorf("connection to the etcd has been closed")
			ch <- &event
			return
		}
	}
}
