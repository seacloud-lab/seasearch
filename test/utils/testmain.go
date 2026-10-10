package utils

import (
	"fmt"
	"os"
	"testing"
)

func RunMain(m *testing.M, setup, cleanup func() error) int {
	dir, err := os.MkdirTemp(".", "testdata-*")
	if err != nil {
		fmt.Printf("failed to make temp dir: %v", err)
		return 1
	}
	defer os.RemoveAll(dir)

	os.Setenv("SS_DATA_PATH", dir)
	os.Setenv("ZINC_FIRST_ADMIN_USER", "admin")
	os.Setenv("ZINC_FIRST_ADMIN_PASSWORD", "Complexpass#123")

	if setup != nil {
		err = setup()
		if err != nil {
			fmt.Printf("failed to setup tests: %v", err)
			return 1
		}
	}
	code := m.Run()
	if cleanup != nil {
		cleanup()
	}
	return code
}
