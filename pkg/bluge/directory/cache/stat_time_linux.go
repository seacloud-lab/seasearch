//go:build linux

package cache

import (
	"syscall"
	"time"
)

func getAtime(st *syscall.Stat_t) time.Time {
	return time.Unix(st.Atim.Sec, st.Atim.Nsec)
}
