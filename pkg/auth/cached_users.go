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

package auth

import (
	"sync"

	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"

	"github.com/dgraph-io/ristretto/z"
	"github.com/rs/zerolog/log"
)

var ZINC_CACHED_USERS = cachedUsers{users: map[string]*meta.User{}}
var closer = z.NewCloser(2)

func Init() {
	if !config.Global.Cluster.Enable {
		return
	}
	go userUpdate()
	go roleUpdate()
}

func Close() {
	if !config.Global.Cluster.Enable {
		return
	}
	closer.SignalAndWait()
}

func userUpdate() {
	defer closer.Done()
	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case <-cluster.UserChan:
			users, err := GetUsers()
			if err != nil {
				log.Err(err).Msg("update user error: cannot get users")
				continue
			}
			ZINC_CACHED_USERS.Put(users)
		}
	}
}

func roleUpdate() {
	defer closer.Done()
	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case <-cluster.RoleChan:
			roles, err := GetRoles()
			if err != nil {
				log.Err(err).Msg("update role error: cannot get roles")
				continue
			}
			ZINC_CACHED_PERMISSIONS.Put(roles)
		}
	}
}

type cachedUsers struct {
	users map[string]*meta.User
	lock  sync.RWMutex
}

func (t *cachedUsers) Get(userID string) (*meta.User, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	user, ok := t.users[userID]
	return user, ok
}

func (t *cachedUsers) Set(userID string, user *meta.User) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.users[userID] = user
}

func (t *cachedUsers) Delete(userID string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.users, userID)
}

func (t *cachedUsers) Put(users []*meta.User) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.users = make(map[string]*meta.User)
	for _, user := range users {
		t.users[user.ID] = user
	}
}
