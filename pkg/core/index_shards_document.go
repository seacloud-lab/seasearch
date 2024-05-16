/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/blugelabs/bluge"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/flatten"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

// BuildBlugeDocumentFromJSON returns the bluge document for the json document. It also updates the mapping for the fields if not found.
// If no mappings are found, it creates te mapping for all the encountered fields. If mapping for some fields is found but not for others
// then it creates the mapping for the missing fields.
func (s *IndexShard) BuildBlugeDocumentFromJSON(docID string, doc map[string]interface{}) (*bluge.Document, map[string][]float32, error) {
	// Pick the index mapping from the cache if it already exists
	mappings := s.root.GetMappings()

	delete(doc, meta.ActionFieldName)
	delete(doc, meta.IDFieldName)
	delete(doc, meta.ShardFieldName)

	// Create a new bluge document
	bdoc := bluge.NewDocument(docID)

	// vector map field -> vector
	vecMap := make(map[string][]float32)

	// Iterate through each field and add it to the bluge document
	for key, value := range doc {
		if value == nil || key == meta.TimeFieldName || key == meta.SourceFieldName {
			continue
		}

		prop, ok := mappings.GetProperty(key)
		if !ok || !prop.Index {
			continue // not index, skip
		}
		if prop.Type == "vector" {
			vector := make([]float32, prop.Dims)
			if array, ok := value.([]interface{}); ok {
				if len(array) != prop.Dims {
					log.Fatal().Msgf("invalid vector field: %s, require Dims %d, got Dims: %d", key, prop.Dims, len(array))
				}
				for i, val := range array {
					f64, ok := val.(float64)
					if !ok {
						log.Fatal().Msgf("invalid vector field: %s, invalid data: %v", key, array)
					}
					vector[i] = float32(f64)
				}
			} else {
				// we should have checked before
				log.Fatal().Msgf("invalid vector field: %s, invalid value: %v", key, value)
			}
			vecMap[key] = vector
			// all vectors are stored in vec_index, so we don't add vector field to bluge doc
			continue
		}
		switch v := value.(type) {
		case []interface{}:
			for _, v := range v {
				if err := s.buildField(mappings, bdoc, key, v); err != nil {
					return nil, nil, err
				}
			}
		default:
			if err := s.buildField(mappings, bdoc, key, v); err != nil {
				return nil, nil, err
			}
		}
	}

	// set timestamp
	timestamp := time.Now()
	if value, ok := doc[meta.TimeFieldName]; ok {
		delete(doc, meta.TimeFieldName)
		// zincSearch use go-json lib and it unmarshal json numbers to float64，
		// but the value of doc would be int64 if we don't use WAL (without serialization)
		switch x := value.(type) {
		case float64:
			timestamp = time.Unix(0, int64(x))
		case int64:
			timestamp = time.Unix(0, x)
		default:
			return nil, nil, fmt.Errorf("invalid timestamp data type")
		}

	}

	// set source
	var sourceByteVal []byte
	if v, ok := doc[meta.SourceFieldName]; ok && v != nil {
		sourceByteVal, _ = json.Marshal(v)
	} else {
		delete(doc, meta.SourceFieldName)
		sourceByteVal, _ = json.Marshal(doc)
	}
	bdoc.AddField(bluge.NewStoredOnlyField("_source", sourceByteVal))

	bdoc.AddField(bluge.NewStoredOnlyField("_index", []byte(s.GetIndexName())))

	// Add time for index
	bdoc.SetTimestamp(timestamp.UnixNano())
	// Upate metadata
	s.SetTimestamp(timestamp.UnixNano())

	return bdoc, vecMap, nil
}

func (s *IndexShard) buildField(mappings *meta.Mappings, bdoc *bluge.Document, key string, value interface{}) error {
	var field *bluge.TermField
	prop, _ := mappings.GetProperty(key)
	switch prop.Type {
	case "text":
		v := value.(string)
		if v == "" {
			return nil
		}
		field = bluge.NewTextField(key, v).SearchTermPositions()
		fieldAnalyzer, _ := zincanalysis.QueryAnalyzerForField(s.root.GetAnalyzers(), mappings, key)
		if fieldAnalyzer != nil {
			field.WithAnalyzer(fieldAnalyzer)
		}
	case "numeric":
		field = bluge.NewNumericField(key, value.(float64))
	case "keyword":
		v := value.(string)
		if v == "" {
			return nil
		}
		field = bluge.NewKeywordField(key, v)
	case "bool":
		field = bluge.NewKeywordField(key, strconv.FormatBool(value.(bool)))
	case "date", "time":
		v, err := zutils.ParseTime(value, prop.Format, prop.TimeZone)
		if err != nil {
			return fmt.Errorf("field [%s] value [%v] parse err: %s", key, value, err.Error())
		}
		field = bluge.NewDateTimeField(key, v)
	}
	if prop.Store || prop.Highlightable {
		field.StoreValue()
	}
	if prop.Highlightable {
		field.HighlightMatches()
	}
	if prop.Sortable {
		field.Sortable()
	}
	if prop.Aggregatable {
		field.Aggregatable()
	}
	bdoc.AddField(field)

	for propField := range prop.Fields {
		err := s.buildField(mappings, bdoc, key+"."+propField, value)
		if err != nil {
			return err
		}
	}

	return nil
}

// CheckDocument checks if the document is valid.
func (s *IndexShard) CheckDocument(docID string, doc map[string]interface{}, update bool, shard int64) ([]byte, error) {
	flatDoc, err := s.CheckDocumentOperation(docID, doc, update, shard)
	if err != nil {
		return nil, err
	}
	return json.Marshal(flatDoc)
}

// CheckDocumentOperation
// checks if the document is valid, and return the operation.
func (s *IndexShard) CheckDocumentOperation(docID string, doc map[string]interface{}, update bool, shard int64) (map[string]interface{}, error) {
	// Pick the index mapping from the cache if it already exists
	mappings := s.root.GetMappings()
	mappingsNeedsUpdate := false

	flatDoc, _ := flatten.Flatten(doc, "")
	// Iterate through each field and add it to the bluge document
	for key, value := range flatDoc {
		if value == nil {
			continue
		}

		if update := s.checkProperty(mappings, key, value); update {
			mappingsNeedsUpdate = true
		}

		prop, ok := mappings.GetProperty(key)
		if !ok || !prop.Index {
			continue // not index, skip
		}
		// check for vector field
		if prop.Type == "vector" {
			if vc, ok := value.([]interface{}); ok {
				if len(vc) != prop.Dims {
					return nil, fmt.Errorf("vector dims should be %d, but got %d", prop.Dims, len(vc))
				}
				for i, v := range vc {
					num, err := zutils.ToFloat64(v)
					if err != nil {
						return nil, fmt.Errorf("invliad vector feild %s: %w", key, err)
					}
					sub := flatDoc[key].([]interface{})
					sub[i] = num
					flatDoc[key] = sub
				}
				// we don't need to store vectors in source
				delete(doc, key)
			}
			continue
		}

		switch v := value.(type) {
		case []interface{}:
			for i, v := range v {
				if err := s.checkField(mappings, flatDoc, key, v, i, true); err != nil {
					return nil, err
				}
			}
		default:
			if err := s.checkField(mappings, flatDoc, key, v, 0, false); err != nil {
				return nil, err
			}
		}
	}

	var err error
	if mappingsNeedsUpdate {
		if err = s.root.SetMappings(mappings); err != nil {
			return nil, err
		}
		if err = StoreIndex(s.root); err != nil {
			return nil, err
		}
	}

	// set timestamp
	timestamp := time.Now()
	if value, ok := flatDoc[meta.TimeFieldName]; ok {
		delete(doc, meta.TimeFieldName)
		prop, _ := mappings.GetProperty(meta.TimeFieldName)
		v, err := zutils.ParseTime(value, prop.Format, prop.TimeZone)
		if err != nil {
			return nil, fmt.Errorf("field [%s] value [%v] parse err: %s", meta.TimeFieldName, value, err.Error())
		}
		timestamp = v
	}

	// prepare for wal
	action := meta.ActionTypeInsert
	if update {
		action = meta.ActionTypeUpdate
	}
	flatDoc[meta.ActionFieldName] = action
	flatDoc[meta.IDFieldName] = docID
	flatDoc[meta.ShardFieldName] = shard
	flatDoc[meta.TimeFieldName] = timestamp.UnixNano()
	flatDoc[meta.SourceFieldName] = doc

	return flatDoc, nil
}

// checkProperty returns if need update mappings
func (s *IndexShard) checkProperty(mappings *meta.Mappings, key string, value interface{}) bool {
	prop, ok := mappings.GetProperty(key)
	if ok {
		if config.Global.EnableTextKeywordMapping && prop.Type == "text" {
			if _, ok := mappings.GetProperty(key + ".keyword"); ok {
				return false
			}
		} else {
			return false
		}
	}

	// try to find the type of the value and use it to define default mapping
	switch v := value.(type) {
	case string:
		if layout, ok := isDateProperty(v); ok {
			prop = meta.NewProperty("date")
			prop.Format = layout
			mappings.SetProperty(key, prop)
		} else {
			newProp := meta.NewProperty("text")
			if config.Global.EnableTextKeywordMapping {
				p := meta.NewProperty("keyword")
				newProp.AddField("keyword", p)
				mappings.SetProperty(key+".keyword", p)
			}
			mappings.SetProperty(key, newProp)
		}
	case int, int64, float64:
		mappings.SetProperty(key, meta.NewProperty("numeric"))
	case bool:
		mappings.SetProperty(key, meta.NewProperty("bool"))
	case []interface{}:
		if v, ok := value.([]interface{}); ok {
			for _, vv := range v {
				switch val := vv.(type) {
				case string:
					if layout, ok := isDateProperty(val); ok {
						prop = meta.NewProperty("date")
						prop.Format = layout
						mappings.SetProperty(key, prop)
					} else {
						newProp := meta.NewProperty("text")
						if config.Global.EnableTextKeywordMapping {
							p := meta.NewProperty("keyword")
							newProp.AddField("keyword", p)
							mappings.SetProperty(key+".keyword", p)
						}
						mappings.SetProperty(key, newProp)
					}
				case float64:
					mappings.SetProperty(key, meta.NewProperty("numeric"))
				case bool:
					mappings.SetProperty(key, meta.NewProperty("bool"))
				}
				break
			}
		}
	}

	return true
}

func (s *IndexShard) checkField(mappings *meta.Mappings, data map[string]interface{}, key string, value interface{}, id int, array bool) error {
	var err error
	var v interface{}
	prop, _ := mappings.GetProperty(key)
	switch prop.Type {
	case "text":
		v, err = zutils.ToString(value)
		if err != nil {
			return fmt.Errorf("field [%s] was set type to [text] but the value [%v] can't convert to string", key, value)
		}
	case "numeric":
		v, err = zutils.ToFloat64(value)
		if err != nil {
			return fmt.Errorf("field [%s] was set type to [numeric] but the value [%v] can't convert to int", key, value)
		}
	case "keyword":
		v, err = zutils.ToString(value)
		if err != nil {
			return fmt.Errorf("field [%s] was set type to [keyword] but the value [%v] can't convert to string", key, value)
		}
	case "bool":
		v, err = zutils.ToBool(value)
		if err != nil {
			return fmt.Errorf("field [%s] was set type to [bool] but the value [%v] can't convert to boolean", key, value)
		}
	case "date", "time":
		_, err := zutils.ParseTime(value, prop.Format, prop.TimeZone)
		if err != nil {
			return fmt.Errorf("field [%s] value [%v] parse err: %s", key, value, err.Error())
		}
		v = value
	}
	if array {
		sub := data[key].([]interface{})
		sub[id] = v
		data[key] = sub
	} else {
		data[key] = v
	}

	return nil
}
