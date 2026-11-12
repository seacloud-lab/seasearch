package store

import (
	"fmt"
	"maps"
	"slices"

	zincerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/mappings"
	"github.com/zincsearch/zincsearch/pkg/vector"
)

// AddMappings adds fields using the Index's frozen analysis configuration and
// synchronously saves metadata. The caller must hold an Index reference. All
// validation precedes mutation; save failures retain the new state and dirty flag.
func (index *Index) AddMappings(request map[string]any) error {
	index.mutex.Lock()
	defer index.mutex.Unlock()

	err := index.checkWritable()
	if err != nil {
		return err
	}
	// Own nested request data before parsing/normalizing it. Never retain mutable
	// maps supplied by the caller in the authoritative metadata.
	added, err := mappings.Request(index.analyzers, copyDocument(request))
	if err != nil {
		return err
	}
	if added == nil || added.Len() == 0 {
		return index.save()
	}
	properties := added.ListProperty()
	fields := make([]string, 0, len(properties))
	for field := range properties {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	newVectors := make(map[string]*meta.VecIndex)
	for _, field := range fields {
		if index.ref.Mappings != nil {
			if _, exists := index.ref.Mappings.GetProperty(field); exists {
				return zincerrors.New(zincerrors.ErrorTypeIllegalArgumentException,
					fmt.Sprintf("index [%s] already exists mapping of field [%s]", index.name, field))
			}
		}
		prop := properties[field]
		if prop.Type != "text" {
			prop.Analyzer = ""
			prop.SearchAnalyzer = ""
		}
		// Nested multi-fields have always been ignored by the mapping endpoint.
		for name, sub := range prop.Fields {
			sub.Fields = nil
			prop.Fields[name] = sub
		}
		properties[field] = prop
		if prop.Type == "vector" {
			// Do not replace a persisted layout, even if its mapping is missing.
			if _, exists := index.ref.VecIndexes[field]; exists {
				return zincerrors.New(zincerrors.ErrorTypeIllegalArgumentException,
					fmt.Sprintf("index [%s] already exists vector field [%s]", index.name, field))
			}
			newVectors[field] = newVectorMetadata(prop)
		}
	}

	index.dirty = true
	if index.ref.Mappings == nil {
		index.ref.Mappings = meta.NewMappings()
	}
	for _, field := range fields {
		index.ref.Mappings.SetProperty(field, properties[field])
	}
	if len(newVectors) > 0 {
		if index.ref.VecIndexes == nil {
			index.ref.VecIndexes = make(map[string]*meta.VecIndex)
		}
		maps.Copy(index.ref.VecIndexes, newVectors)
		index.vectorLayoutChecked = false
	}
	return index.save()
}

// newVectorMetadata constructs only a new field's initial layout. It must not be
// used to reinitialize existing fields during mapping updates.
func newVectorMetadata(prop meta.Property) *meta.VecIndex {
	return &meta.VecIndex{
		StoreWithHash:    true,
		TargetType:       prop.VecIndexType,
		Dims:             prop.Dims,
		NBits:            prop.NBits,
		M:                prop.M,
		StoreWithFloat16: prop.StoreWithFloat16,
		LayoutVersion:    vector.CurLayoutVersion,
		Segments:         []*meta.VectorSegment{{Id: 0, Count: 0, Status: vector.StatusGrowing}},
	}
}
