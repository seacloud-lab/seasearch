package auth

import (
	"os"
	"testing"

	"github.com/zincsearch/zincsearch/pkg/auth"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/test/utils"
)

func TestMain(m *testing.M) {
	start := func() error {
		config.InitConfig()
		metadata.InitMetaStorage()
		ider.InitIder()
		auth.InitFirstUser()
		return nil
	}
	os.Exit(utils.RunMain(m, start, nil))
}
