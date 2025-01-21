package auth

import (
	"os"
	"testing"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/metadata"
)

func TestMain(m *testing.M) {
	config.InitConfig()
	ider.InitIder()
	metadata.InitMetaStorage()
	InitFirstUser()
	os.Exit(m.Run())
}
