package text

import (
	"fmt"
	"os"
	"path"

	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/hash/rendezvous"

	"github.com/blugelabs/bluge/analysis"
)

// Index owns text runtime resources, not persistent metadata. Metadata passed
// to its methods is borrowed; the caller synchronizes access to that metadata.
type Index struct {
	// The first-shard collection and routing table are immutable after New.
	shardHashing *rendezvous.Rendezvous
	shards       map[string]*IndexShard
}

type Options struct {
	Name        string
	StoreName   string
	StorageType string
	ReadOnly    bool

	// The analyzer is shared with Bluge and must remain immutable after New.
	DefaultSearchAnalyzer *analysis.Analyzer
}

// New borrows shard metadata to initialize runtime shards and routing without
// retaining metadata references or opening storage. The caller protects metadata
// access for the duration of this call.
// StoreName is the already resolved hashed or legacy plain storage path.
func New(options Options, shards map[string]*meta.IndexShard) (*Index, error) {
	if options.Name == "" || options.StoreName == "" {
		return nil, fmt.Errorf("index name and store name must not be empty")
	}
	switch options.StorageType {
	case "disk", "s3", "oss":
	default:
		return nil, fmt.Errorf("invalid storage type: %s", options.StorageType)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("index %s has no first shards", options.Name)
	}
	index := &Index{
		shardHashing: rendezvous.New(),
		shards:       make(map[string]*IndexShard, len(shards)),
	}
	for id, shard := range shards {
		if shard == nil || shard.ID != id || id == "" {
			return nil, fmt.Errorf("invalid first shard %q", id)
		}
		if shard.ShardNum <= 0 || shard.ShardNum != int64(len(shard.Shards)) {
			return nil, fmt.Errorf("invalid second shard count for %s/%s", options.Name, id)
		}
		first := &IndexShard{
			id:      id,
			name:    path.Join(options.Name, id),
			options: options,
			shards:  make([]*IndexSecondShard, len(shard.Shards)),
		}
		for i, second := range shard.Shards {
			if second == nil || second.ID != int64(i) {
				return nil, fmt.Errorf("invalid second shard %s/%s/%d", options.Name, id, i)
			}
			first.shards[i] = &IndexSecondShard{id: second.ID}
		}
		index.shards[id] = first
		index.shardHashing.Add(id)
	}
	return index, nil
}

// Close closes all cached Writers. The caller must first drain Index references
// and close all request Readers. This is resource cleanup, not reference release.
// Writer cleanup failures are logged by their owning shards, not returned.
func (index *Index) Close() {
	for _, shard := range index.shards {
		shard.Close()
	}
}

// Delete removes persistent text data after all owner resources have closed.
// storeName is the validated hashed or legacy plain index storage path.
func Delete(storeName, storageType string) error {
	if storageType != config.Global.StorageType {
		return fmt.Errorf("configured storage backend is %s, index uses %s", config.Global.StorageType, storageType)
	}
	switch storageType {
	case "disk":
		return os.RemoveAll(path.Join(config.Global.DataPath, storeName))
	case "s3":
		// The slash prevents a legacy name from matching other index prefixes.
		return directory.RemoveS3Index(storeName + "/")
	case "oss":
		return directory.RemoveOssIndex(storeName + "/")
	default:
		return fmt.Errorf("invalid storage type: %s", storageType)
	}
}
