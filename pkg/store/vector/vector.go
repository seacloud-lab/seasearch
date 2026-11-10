package vector

import (
	"fmt"
	"os"
	"path"

	"github.com/zincsearch/zincsearch/pkg/config"
	vectorstore "github.com/zincsearch/zincsearch/pkg/vector"
)

// Delete removes all vector fields of an index without opening vector resources.
// storeName is the validated hashed or legacy plain index storage path. Callers
// must first close any runtime vector resources before removing their storage.
func Delete(storeName, storageType string) error {
	if storageType != config.Global.StorageType {
		return fmt.Errorf("configured storage backend is %s, index uses %s", config.Global.StorageType, storageType)
	}
	store, err := vectorstore.GetVectorStorage()
	if err != nil {
		return err
	}
	if err := store.Remove(storeName); err != nil {
		return err
	}
	return os.RemoveAll(path.Join(config.Global.DataPath, vectorstore.VecPrefix, storeName))
}
