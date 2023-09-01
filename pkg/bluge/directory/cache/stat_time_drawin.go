//go:build darwin

package cache

import (
	"syscall"
	"time"
)

func getAtime(st *syscall.Stat_t) time.Time {
	return time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec)
}
