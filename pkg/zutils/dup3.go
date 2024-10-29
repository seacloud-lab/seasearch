//go:build linux && arm64

package zutils

import (
	"syscall"
)

func Dup(from, to int) error {
	return syscall.Dup3(from, to, 0)
}
