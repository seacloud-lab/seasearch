package index

import (
	"os"
	"testing"

	"github.com/zincsearch/zincsearch/pkg/auth"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/test/storetest"
	"github.com/zincsearch/zincsearch/test/utils"
)

func TestMain(m *testing.M) {
	start := func() error {
		config.InitConfig()
		metadata.InitMetaStorage()
		ider.InitIder()
		auth.InitFirstUser()
		return storetest.Start()
	}
	stop := func() error {
		storetest.Stop()
		return metadata.Close()
	}
	os.Exit(utils.RunMain(m, start, stop))
}
