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

package mappings

import (
	"fmt"
	"strings"

	"github.com/zincsearch/zincsearch/pkg/core/vector"

	"github.com/blugelabs/bluge/analysis"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

func Request(analyzers map[string]*analysis.Analyzer, data map[string]interface{}) (*meta.Mappings, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if data["properties"] == nil {
		return nil, errors.New(errors.ErrorTypeParsingException, "[mappings] properties should be defined")
	}

	properties, ok := data["properties"].(map[string]interface{})
	if !ok {
		return nil, errors.New(errors.ErrorTypeParsingException, "[mappings] properties should be an object")
	}

	mappings := meta.NewMappings()
	for field, prop := range properties {
		var propFields map[string]interface{}

		prop, ok := prop.(map[string]interface{})
		if !ok {
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] properties [%s] should be an object", field))
		}

		if v, ok := prop["properties"]; ok {
			if _, ok := v.(map[string]interface{}); !ok {
				return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] properties [%s] should be an object", field))
			}

			if subMappings, err := Request(analyzers, prop); err == nil {
				for k, v := range subMappings.ListProperty() {
					mappings.SetProperty(field+"."+k, v)
				}
			} else {
				return nil, err
			}

			continue
		}

		if v, ok := prop["fields"]; ok {
			if propFields, ok = v.(map[string]interface{}); !ok {
				return nil, errors.New(errors.ErrorTypeParsingException,
					fmt.Sprintf("[mappings] property.fields [%s] should be an object, got %T", field, v))
			}
		}

		propType, ok := prop["type"]
		if !ok {
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] properties [%s] should be exists", "type"))
		}

		propTypeStr, ok := propType.(string)
		if !ok {
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] properties [%s] should be an string", "type"))
		}

		var newProp meta.Property
		propTypeStr = strings.ToLower(propTypeStr)
		switch propTypeStr {
		case "text":
			newProp = meta.NewProperty(propTypeStr)

			if config.Global.EnableTextKeywordMapping {
				p := meta.NewProperty("keyword")
				newProp.AddField("keyword", p)
			}
		case "keyword", "numeric", "bool", "date":
			newProp = meta.NewProperty(propTypeStr)
		case "constant_keyword":
			newProp = meta.NewProperty("keyword")
		case "match_only_text":
			newProp = meta.NewProperty("text")
		case "integer", "double", "long", "short", "int", "float":
			newProp = meta.NewProperty("numeric")
		case "boolean":
			newProp = meta.NewProperty("bool")
		case "time", "datetime":
			newProp = meta.NewProperty("date")
		case "vector":
			newProp = meta.NewProperty("vector")
			newProp.Store = true
		case "flattened", "object", "nested", "wildcard", "byte", "alias", "geo_point", "ip", "ip_range", "scaled_float":
			// ignore
		default:
			return nil, errors.New(errors.ErrorTypeXContentParseException, fmt.Sprintf("[mappings] properties [%s] doesn't support type [%s]", field, propTypeStr))
		}

		for k, v := range prop {
			switch k {
			case "type":
				// handled
			case "analyzer":
				newProp.Analyzer = v.(string)
			case "search_analyzer":
				newProp.SearchAnalyzer = v.(string)
			case "format":
				newProp.Format = v.(string)
			case "time_zone":
				newProp.TimeZone = v.(string)
				_, err := zutils.ParseTimeZone(newProp.TimeZone)
				if err != nil {
					return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] %s time_zone parse err %s", field, err.Error()))
				}
			case "index":
				newProp.Index = v.(bool)
			case "store":
				newProp.Store = v.(bool)
			case "sortable":
				newProp.Sortable = v.(bool)
			case "aggregatable":
				newProp.Aggregatable = v.(bool)
			case "highlightable":
				newProp.Highlightable = v.(bool)
			case "dims":
				var err error
				newProp.Dims, err = zutils.ToInt(v)
				if err != nil {
					return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] %s dims parse err %s", field, err.Error()))
				}
			case "nbits":
				var err error
				newProp.NBits, err = zutils.ToInt(v)
				if err != nil {
					return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] %s nbits parse err %s", field, err.Error()))
				}
			case "m":
				var err error
				newProp.M, err = zutils.ToInt(v)
				if err != nil {
					return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] %s M parse err %s", field, err.Error()))
				}
			case "vec_index_type":
				newProp.VecIndexType = v.(string)
			case "store_with_float16":
				var err error
				newProp.StoreWithFloat16, err = zutils.ToBool(v)
				if err != nil {
					return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] %s store_with_float16 parse err %s", field, err.Error()))
				}
			default:
				// ignore unknown options
				// return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] properties [%s] unknown option [%s]", field, k))
			}
		}

		if newProp.Highlightable {
			newProp.Store = true
		}

		if newProp.Type != "" {
			mappings.SetProperty(field, newProp)
		}
		// check settings
		if newProp.Type == "vector" {
			if newProp.Dims <= 0 {
				return nil, errors.New(errors.ErrorTypeInvalidArgument, fmt.Sprintf("[mappings] %s dims should greater than 0", field))
			}
			if newProp.VecIndexType == vector.IvfPQ {
				if newProp.M <= 0 {
					return nil, errors.New(errors.ErrorTypeInvalidArgument, fmt.Sprintf("[mappings] %s m should greater than 0", field))
				}
				if newProp.NBits <= 0 {
					return nil, errors.New(errors.ErrorTypeInvalidArgument, fmt.Sprintf("[mappings] %s NBits should greater than 0", field))
				}
				if newProp.Dims%newProp.M != 0 {
					return nil, errors.New(errors.ErrorTypeInvalidArgument, fmt.Sprintf("[mappings] %s dims should be divisible by m", field))
				}
				// ivf_pq doesn't need store with float16.
				newProp.StoreWithFloat16 = false
			}
			if newProp.VecIndexType != vector.Flat && newProp.VecIndexType != vector.IvfPQ {
				return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[mappings] %s vec_index_type parse err invalid vec type", field))
			}
		}
		if newProp.Type == "text" {
			fields, err := convertToField(propFields)
			if err != nil {
				return nil, err
			}

			for k, v := range fields {
				newProp.AddField(k, v)
				mappings.SetProperty(field+"."+k, v)
			}

			// check analyzer
			if newProp.Analyzer != "" {
				if _, err := zincanalysis.QueryAnalyzer(analyzers, newProp.Analyzer); err != nil {
					return nil, err
				}
			}
			if newProp.SearchAnalyzer != "" {
				if _, err := zincanalysis.QueryAnalyzer(analyzers, newProp.SearchAnalyzer); err != nil {
					return nil, err
				}
			}
		}
	}

	return mappings, nil
}

// convertToField converst v to type map[string]meta.Property.
func convertToField(v map[string]interface{}) (map[string]meta.Property, error) {
	r := make(map[string]meta.Property)

	if len(v) == 0 || v == nil {
		return r, nil
	}

	// it seems inefficient to encode and then directly decode it.
	// But in favor of code maintainability and to prevent that we need to maintain
	// duplicated code its more "efficient" to parse the fields this way.

	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}

	return r, nil
}
