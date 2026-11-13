package catalog

import (
	"os"
	"testing"

	"github.com/zincsearch/zincsearch/pkg/auth"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/store"
	"github.com/zincsearch/zincsearch/test/utils"
)

func TestMain(m *testing.M) {
	start := func() error {
		config.InitConfig()
		metadata.InitMetaStorage()
		ider.InitIder()
		auth.InitFirstUser()
		aliases, err := metadata.Alias.Get()
		if err != nil {
			return err
		}
		ZINC_INDEX_ALIAS_LIST = *NewAliasList()
		if aliases != nil {
			ZINC_INDEX_ALIAS_LIST.Aliases = aliases
		}
		// Retained template/telemetry tests use store without legacy workers.
		return store.Start()
	}
	stop := func() error {
		store.Stop()
		return metadata.Close()
	}
	os.Exit(utils.RunMain(m, start, stop))
}
