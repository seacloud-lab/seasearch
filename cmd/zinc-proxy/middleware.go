package main

import (
	"encoding/base64"
	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/auth"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"golang.org/x/crypto/argon2"
	"net/http"
	"strings"
	"sync"
)

// AuthMiddlewareNoCache
// Proxy authenticates directly through etcd
func AuthMiddlewareNoCache(permission string) func(c *gin.Context) {
	auth.AddPermission(permission)
	return func(c *gin.Context) {
		// Get the Basic Authentication credentials
		user, password, hasAuth := c.Request.BasicAuth()
		if hasAuth {
			if u, ok := VerifyCredentials(user, password); ok {
				if VerifyRoleHasPermission(u.Role, permission) {
					c.Next()
				} else {
					c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "No permission:" + permission})
					return
				}
			} else {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"auth": "Invalid credentials"})
				return
			}
		} else {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"auth": "Missing credentials"})
			return
		}
	}
}

func VerifyCredentials(userID, password string) (*meta.User, bool) {
	userID = strings.ToLower(userID)
	user, err := metadata.User.Get(userID)
	if err != nil {
		return user, false
	}

	incomingEncryptedPassword := GeneratePassword(password, user.Salt)
	if incomingEncryptedPassword == user.Password {
		return user, true
	}

	return nil, false
}

func VerifyRoleHasPermission(roleId, permission string) bool {
	roleId = strings.ToLower(roleId)
	if roleId == "admin" {
		return true
	}
	role, err := metadata.Role.Get(roleId)
	if err != nil {
		return false
	}
	pm := strArrayToMap(role.Permission)
	_, ok := pm[permission]
	return ok
}

var _cacheGeneratePassword = sync.Map{}

func GeneratePassword(password, salt string) string {
	key := password + ":" + salt
	if v, ok := _cacheGeneratePassword.Load(key); ok {
		return v.(string)
	}
	params := &Argon2Params{
		Memory:      2 * 1024,
		Iterations:  3,
		Parallelism: 2,
		SaltLength:  128,
		KeyLength:   32,
		Time:        1,
		Threads:     1,
	}
	hash := argon2.IDKey([]byte(password), []byte(salt), params.Time, params.Memory, params.Threads, params.KeyLength)
	val := base64.StdEncoding.EncodeToString(hash)
	_cacheGeneratePassword.Store(key, val)
	return val
}

type Argon2Params struct {
	Time        uint32
	Memory      uint32
	Threads     uint8
	KeyLength   uint32
	SaltLength  uint32
	Parallelism uint8
	Iterations  uint32
}

func strArrayToMap(ss []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, v := range ss {
		m[v] = struct{}{}
	}
	return m
}
