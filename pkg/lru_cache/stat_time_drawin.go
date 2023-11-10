//go:build darwin

package lru_cache

import (
	"syscall"
	"time"
)

func GetAtime(st *syscall.Stat_t) time.Time {
	return time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec)
}
