package storetest

import (
	"github.com/zincsearch/zincsearch/pkg/catalog"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/store"
)

// Start initializes aliases and the store without starting legacy runtime,
// WAL or asynchronous metadata workers. Call after metadata initialization.
func Start() error {
	aliases, err := metadata.Alias.Get()
	if err != nil {
		return err
	}
	catalog.ZINC_INDEX_ALIAS_LIST = *catalog.NewAliasList()
	if aliases != nil {
		catalog.ZINC_INDEX_ALIAS_LIST.Aliases = aliases
	}
	if err := store.Start(); err != nil {
		return err
	}
	store.StartVectorMaintenance()
	return nil
}

// Stop drains maintenance before closing runtimes. Test requests must have
// finished and released their references before calling it; metadata closes last.
func Stop() {
	store.StopVectorMaintenance()
	store.Stop()
}
