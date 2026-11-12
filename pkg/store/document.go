package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/store/text"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
	"github.com/zincsearch/zincsearch/pkg/zutils/flatten"
)

// prepareDocumentLocked requires mu's write lock. The caller resolves the target
// second shard and whether this is an insert or update before calling it. Mapping
// changes remain dirty even if a later field fails validation; this helper does
// not save metadata or open Writers. The enclosing write operation handles both.
func (index *Index) prepareDocumentLocked(docID string, doc map[string]interface{}, update bool, shardID int64) (text.Operation, error) {
	if index.readOnly {
		return text.Operation{}, text.ErrReadOnly
	}
	if len(index.ref.VecIndexes) > 0 && base62.Encode(base62.Decode(docID)) != docID {
		return text.Operation{}, fmt.Errorf("document ID (%v) is not a valid base62 string", docID)
	}
	// Own the request data: flatten retains scalar arrays, and normalization must
	// not change the caller's source or another operation in the same request.
	source := copyDocument(doc)
	fields, err := flatten.Flatten(source, "")
	if err != nil {
		return text.Operation{}, err
	}
	mappings := index.ref.Mappings
	for key, value := range fields {
		if value == nil {
			continue
		}
		if inferProperty(mappings, key, value) {
			index.dirty = true
		}
		prop, ok := mappings.GetProperty(key)
		if !ok || !prop.Index {
			continue
		}
		if prop.Type == "vector" {
			values, ok := value.([]interface{})
			if !ok {
				return text.Operation{}, fmt.Errorf("invalid vector field %s: expected an array", key)
			}
			if len(values) != prop.Dims {
				return text.Operation{}, fmt.Errorf("vector dims should be %d, but got %d", prop.Dims, len(values))
			}
			for i, value := range values {
				values[i], err = zutils.ToFloat64(value)
				if err != nil {
					return text.Operation{}, fmt.Errorf("invalid vector field %s: %w", key, err)
				}
			}
			fields[key] = values
			// Preserve the existing source rule: remove top-level vector fields.
			delete(source, key)
			continue
		}
		if values, ok := value.([]interface{}); ok {
			for i, value := range values {
				values[i], err = normalizeField(prop, key, value)
				if err != nil {
					return text.Operation{}, err
				}
			}
			fields[key] = values
		} else {
			fields[key], err = normalizeField(prop, key, value)
			if err != nil {
				return text.Operation{}, err
			}
		}
	}

	timestamp := time.Now()
	if value, ok := fields[meta.TimeFieldName]; ok {
		prop, _ := mappings.GetProperty(meta.TimeFieldName)
		timestamp, err = zutils.ParseTime(value, prop.Format, prop.TimeZone)
		if err != nil {
			return text.Operation{}, fmt.Errorf("field [%s] value [%v] parse err: %w", meta.TimeFieldName, value, err)
		}
		delete(source, meta.TimeFieldName)
	}
	// Control fields are represented by typed operation fields, not user data.
	for _, key := range []string{meta.ActionFieldName, meta.IDFieldName, meta.ShardFieldName, meta.TimeFieldName, meta.SourceFieldName} {
		delete(fields, key)
	}
	action := meta.ActionTypeInsert
	if update {
		action = meta.ActionTypeUpdate
	}
	return text.Operation{
		DocID: docID, Action: action, ShardID: shardID,
		Document: &text.Document{Fields: fields, Source: source, Timestamp: timestamp.UnixNano()},
	}, nil
}

func copyDocument(doc map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(doc))
	for key, value := range doc {
		result[key] = copyDocumentValue(value)
	}
	return result
}

func copyDocumentValue(value interface{}) interface{} {
	switch value := value.(type) {
	case map[string]interface{}:
		return copyDocument(value)
	case []interface{}:
		result := make([]interface{}, len(value))
		for i, value := range value {
			result[i] = copyDocumentValue(value)
		}
		return result
	default:
		return value
	}
}

// inferProperty follows the legacy first-array-element and date detection rules.
// Existing fields are preserved except for the legacy text.keyword completion.
func inferProperty(mappings *meta.Mappings, key string, value interface{}) bool {
	if prop, ok := mappings.GetProperty(key); ok {
		if !config.Global.EnableTextKeywordMapping || prop.Type != "text" {
			return false
		}
		if _, ok := mappings.GetProperty(key + ".keyword"); ok {
			return false
		}
	}
	if values, ok := value.([]interface{}); ok {
		if len(values) == 0 {
			return false
		}
		value = values[0]
		// Legacy array inference recognizes JSON numbers, not integer values.
		switch value.(type) {
		case string, float64, bool:
		default:
			return false
		}
	}
	var prop meta.Property
	switch value := value.(type) {
	case string:
		if layout := detectTimeLayout(value); layout != "" {
			if _, err := time.Parse(layout, value); err == nil {
				prop = meta.NewProperty("date")
				prop.Format = layout
			}
		}
		if prop.Type == "" {
			prop = meta.NewProperty("text")
			if config.Global.EnableTextKeywordMapping {
				keyword := meta.NewProperty("keyword")
				prop.AddField("keyword", keyword)
				mappings.SetProperty(key+".keyword", keyword)
			}
		}
	case int, int64, float64:
		prop = meta.NewProperty("numeric")
	case bool:
		prop = meta.NewProperty("bool")
	default:
		return false
	}
	mappings.SetProperty(key, prop)
	return true
}

func normalizeField(prop meta.Property, key string, value interface{}) (interface{}, error) {
	var normalized interface{}
	var err error
	switch prop.Type {
	case "text", "keyword":
		normalized, err = zutils.ToString(value)
	case "numeric":
		normalized, err = zutils.ToFloat64(value)
	case "bool":
		normalized, err = zutils.ToBool(value)
	case "date", "time":
		_, err = zutils.ParseTime(value, prop.Format, prop.TimeZone)
		normalized = value
	default:
		return nil, fmt.Errorf("field [%s] has unsupported type [%s]", key, prop.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("field [%s] value [%v] cannot convert to [%s]: %w", key, value, prop.Type, err)
	}
	return normalized, nil
}

func detectTimeLayout(value string) string {
	switch {
	case len(value) == 19 && strings.Index(value, " ") == 10:
		return "2006-01-02 15:04:05"
	case len(value) == 19 && strings.Index(value, "T") == 10:
		return "2006-01-02T15:04:05"
	case len(value) == 25 && strings.Index(value, "T") == 10:
		return time.RFC3339
	case len(value) == 29 && strings.Index(value, "T") == 10 && strings.Index(value, ".") == 19:
		return "2006-01-02T15:04:05.999Z07:00"
	default:
		return ""
	}
}
