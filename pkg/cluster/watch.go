package cluster

import (
	"context"
	"fmt"

	"github.com/zincsearch/zincsearch/pkg/config"

	"github.com/haiwen/goutils/clusterkit"
)

func WatchUsers(ctx context.Context) clusterkit.Iter[int64] {
	key := fmt.Sprintf("%s/metadata/user/", config.Global.Etcd.Prefix)
	return clusterkit.WatchUpdates(ctx, 0, key, true)
}

func WatchRoles(ctx context.Context) clusterkit.Iter[int64] {
	key := fmt.Sprintf("%s/metadata/role/", config.Global.Etcd.Prefix)
	return clusterkit.WatchUpdates(ctx, 0, key, true)
}
