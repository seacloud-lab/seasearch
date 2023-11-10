//go:build linux

package lru_cache

import (
	"syscall"
	"time"
)

func GetAtime(st *syscall.Stat_t) time.Time {
	return time.Unix(st.Atim.Sec, st.Atim.Nsec)
}
