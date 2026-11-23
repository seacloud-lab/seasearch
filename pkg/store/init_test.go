package store

import (
	"os"
	"testing"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/test/utils"
)

func TestMain(m *testing.M) {
	start := func() error {
		// RunMain supplies a disposable data directory; force local backends so
		// developer environment settings cannot direct tests to shared storage.
		for key, value := range map[string]string{
			"SS_STORAGE_TYPE":       "disk",
			"ZINC_METADATA_STORAGE": "bolt",
			"SS_CLUSTER_ENABLE":     "false",
		} {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}

		config.InitConfig()
		metadata.InitMetaStorage()
		ider.InitIder()
		return Start()
	}
	stop := func() error {
		Stop()
		return metadata.Close()
	}
	os.Exit(utils.RunMain(m, start, stop))
}
