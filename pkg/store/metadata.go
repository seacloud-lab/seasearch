package store

import (
	"errors"
	"fmt"
	"maps"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/ider"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	storevector "github.com/zincsearch/zincsearch/pkg/store/vector"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/uquery/mappings"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"

	"github.com/blugelabs/bluge/analysis"
)

// readIndex constructs an unpublished runtime index. The caller coordinates
// loading and cache publication; this function does not write metadata.
func readIndex(name string, readOnly bool) (*Index, error) {
	ref, err := metadata.Index.Get(name)
	if errors.Is(err, zincerrors.ErrKeyNotFound) {
		return nil, fmt.Errorf("index %s: %w", name, ErrIndexNotFound)
	} else if err != nil {
		return nil, fmt.Errorf("read index %s metadata: %w", name, err)
	}
	if ref == nil || ref.Name != name {
		return nil, fmt.Errorf("index %s metadata has an invalid name", name)
	}
	index, err := formatIndex(ref, readOnly)
	if err != nil {
		return nil, fmt.Errorf("format index %s: %w", name, err)
	}
	return index, nil
}

// writeIndex synchronously saves unsaved metadata under the same lock used for
// mutations. It does not publish the Index or acquire lifecycle locks. The caller
// must not hold index.mutex; already-locked write paths use save instead.
func writeIndex(index *Index) error {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	return index.save()
}

// save requires the caller to hold index.mutex's write lock through persistence
// and dirty state updates; it does not acquire the lock itself. It must not call
// MarshalJSON, which would recursively acquire index.mutex.
// Failures retain both the current metadata and its dirty flag for a later retry.
func (index *Index) save() error {
	if index.readOnly {
		return fmt.Errorf("save index %s metadata: %w", index.name, text.ErrReadOnly)
	}
	if !index.dirty {
		return nil
	}

	data, err := json.Marshal(index.ref)
	if err != nil {
		return fmt.Errorf("serialize index %s metadata: %w", index.name, err)
	}
	err = metadata.Index.Set(index.name, data)
	if err != nil {
		return fmt.Errorf("save index %s metadata: %w", index.name, err)
	}
	index.dirty = false
	return nil
}

// createIndex initializes metadata and runtime objects without opening storage
// or publishing a cache reference. All allocations on failure are GC-managed.
func createIndex(request *meta.IndexSimple) (*Index, error) {
	name := request.Name
	err := core.CheckIndexName(name)
	if err != nil {
		return nil, err
	}
	// Analyzer and mapping normalization can mutate nested input. Own all input
	// state before applying it, including maps shared with the caller.
	data, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("copy index %s request: %w", name, err)
	}
	var owned meta.IndexSimple
	err = json.Unmarshal(data, &owned)
	if err != nil {
		return nil, fmt.Errorf("copy index %s request: %w", name, err)
	}
	ref := &meta.Index{
		Name:          name,
		StorageType:   config.Global.StorageType,
		Version:       meta.Version,
		StoreWithHash: true,
		ShardNum:      1,
	}

	template, err := core.UseTemplate(name)
	if err != nil {
		return nil, fmt.Errorf("load template for index %s: %w", name, err)
	}
	if template != nil {
		// JSON copying preserves all property fields and detaches nested maps and
		// slices before mapping normalization and analyzer construction mutate them.
		data, err := json.Marshal(template.Template)
		if err != nil {
			return nil, fmt.Errorf("copy template for index %s: %w", name, err)
		}
		var settings meta.TemplateTemplate
		err = json.Unmarshal(data, &settings)
		if err != nil {
			return nil, fmt.Errorf("copy template for index %s: %w", name, err)
		}
		ref.Settings = settings.Settings
		ref.Mappings = settings.Mappings
	}
	if ref.Settings == nil {
		ref.Settings = new(meta.IndexSettings)
	}
	if owned.Settings != nil && owned.Settings.Analysis != nil {
		if ref.Settings.Analysis == nil {
			ref.Settings.Analysis = new(meta.IndexAnalysis)
		}
		dst, src := ref.Settings.Analysis, owned.Settings.Analysis
		// Normalize aliases in each source before merging, so request aliases
		// override template entries just like their canonical spelling.
		if dst.TokenFilter == nil {
			dst.TokenFilter = dst.Filter
		}
		if src.TokenFilter == nil {
			src.TokenFilter = src.Filter
		}
		dst.Filter = nil
		dst.Analyzer = mergeNamed(dst.Analyzer, src.Analyzer)
		dst.CharFilter = mergeNamed(dst.CharFilter, src.CharFilter)
		dst.Tokenizer = mergeNamed(dst.Tokenizer, src.Tokenizer)
		dst.TokenFilter = mergeNamed(dst.TokenFilter, src.TokenFilter)
	}
	analyzers, err := zincanalysis.RequestAnalyzer(ref.Settings.Analysis)
	if err != nil {
		return nil, fmt.Errorf("parse index analysis: %w", err)
	}
	requestedMappings, err := mappings.Request(analyzers, owned.Mappings)
	if err != nil {
		return nil, err
	}
	if requestedMappings != nil {
		if ref.Mappings == nil {
			ref.Mappings = meta.NewMappings()
		}
		for field, prop := range requestedMappings.ListProperty() {
			ref.Mappings.SetProperty(field, prop)
		}
	}
	initializeMappings(ref)

	node, err := ider.NewNode(0)
	if err != nil {
		return nil, fmt.Errorf("create shard ID for index %s: %w", name, err)
	}
	id := node.Generate()
	ref.Shards = map[string]*meta.IndexShard{
		id: {
			ID:       id,
			ShardNum: 1,
			Shards:   []*meta.IndexSecondShard{{ID: 0}},
		},
	}
	index, err := formatIndexWithAnalyzers(ref, false, analyzers)
	if err != nil {
		return nil, fmt.Errorf("initialize index %s: %w", name, err)
	}
	// The instance is still unpublished and exclusively owned by this call.
	// Its initial metadata must be saved before it enters the cache.
	index.dirty = true
	return index, nil
}

func mergeNamed[T any](dst, src map[string]T) map[string]T {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[string]T, len(src))
	}
	maps.Copy(dst, src)
	return dst
}

// formatIndex takes ownership of ref, including its nested mutable state. It
// only allocates Go objects: Writers and vector resources remain lazily opened,
// so failures need neither resource cleanup nor metadata rollback.
func formatIndex(ref *meta.Index, readOnly bool) (*Index, error) {
	if ref == nil {
		return nil, fmt.Errorf("index metadata must not be nil")
	}
	if ref.Settings == nil {
		ref.Settings = new(meta.IndexSettings)
	}
	analyzers, err := zincanalysis.RequestAnalyzer(ref.Settings.Analysis)
	if err != nil {
		return nil, fmt.Errorf("parse index analysis: %w", err)
	}
	return formatIndexWithAnalyzers(ref, readOnly, analyzers)
}

// formatIndexWithAnalyzers shares the final analyzer objects with text runtime
// options. Creation parses mappings with this same set before constructing text.
func formatIndexWithAnalyzers(ref *meta.Index, readOnly bool, analyzers map[string]*analysis.Analyzer) (*Index, error) {
	err := core.CheckIndexName(ref.Name)
	if err != nil {
		return nil, err
	}
	if ref.ShardNum <= 0 || ref.ShardNum != int64(len(ref.Shards)) {
		return nil, fmt.Errorf("invalid first shard count for index %s", ref.Name)
	}
	if ref.Settings == nil {
		ref.Settings = new(meta.IndexSettings)
	}
	if ref.Mappings == nil {
		ref.Mappings = meta.NewMappings()
		ref.Mappings.SetProperty(meta.TimeFieldName, meta.NewProperty("date"))
	}
	if ref.Mappings.Properties == nil {
		ref.Mappings.Properties = make(map[string]meta.Property)
	}
	if analyzers == nil {
		analyzers = make(map[string]*analysis.Analyzer)
	}
	index := &Index{
		name:      ref.Name,
		ref:       ref,
		analyzers: analyzers,
		readOnly:  readOnly,
	}
	index.text, err = text.New(text.Options{
		Name:                  ref.Name,
		StoreName:             index.GetStoreName(),
		StorageType:           ref.StorageType,
		ReadOnly:              readOnly,
		DefaultSearchAnalyzer: analyzers["default"],
	}, ref.Shards)
	if err != nil {
		return nil, err
	}
	if !readOnly {
		index.vector = storevector.NewVecIndexManager(storevector.Options{
			Name:        ref.Name,
			StoreName:   index.GetStoreName(),
			StorageType: ref.StorageType,
		})
	}
	return index, nil
}

// initializeMappings applies creation-time rules only. Loading existing indexes
// must preserve their persisted mappings and vector layouts.
func initializeMappings(ref *meta.Index) {
	if ref.Mappings == nil {
		ref.Mappings = meta.NewMappings()
	}
	if ref.Mappings.Properties == nil {
		ref.Mappings.Properties = make(map[string]meta.Property)
	}
	for field, prop := range ref.Mappings.ListProperty() {
		if prop.Type != "text" {
			prop.Analyzer = ""
			prop.SearchAnalyzer = ""
			ref.Mappings.SetProperty(field, prop)
		}
		if prop.Type == "vector" {
			if ref.VecIndexes == nil {
				ref.VecIndexes = make(map[string]*meta.VecIndex)
			}
			ref.VecIndexes[field] = newVectorMetadata(prop)
		}
	}
	ref.Mappings.SetProperty("_id", meta.NewProperty("keyword"))
	timestamp, ok := ref.Mappings.GetProperty(meta.TimeFieldName)
	if !ok {
		timestamp = meta.NewProperty("date")
	}
	timestamp.Index = true
	timestamp.Sortable = true
	timestamp.Aggregatable = true
	ref.Mappings.SetProperty(meta.TimeFieldName, timestamp)
}
