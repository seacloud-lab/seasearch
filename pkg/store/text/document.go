package text

import (
	"fmt"
	"strconv"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/zincsearch/zincsearch/pkg/meta"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

// Document is request-owned normalized data. It contains no borrowed metadata.
// Timestamp is in nanoseconds, and Fields excludes operation control fields.
type Document struct {
	Fields    map[string]interface{}
	Source    map[string]interface{}
	Timestamp int64
}

// BuildDocument borrows mappings and immutable analyzers only during this call.
// It neither modifies metadata nor opens resources. The timestamp remains in
// Document so the caller can update metadata after a successful Batch.
func BuildDocument(indexName, docID string, doc *Document, mappings *meta.Mappings, analyzers map[string]*analysis.Analyzer) (*bluge.Document, map[string][]float32, error) {
	if doc == nil {
		return nil, nil, fmt.Errorf("document %s has no prepared data", docID)
	}
	bdoc := bluge.NewDocument(docID)
	vectors := make(map[string][]float32)
	for key, value := range doc.Fields {
		if value == nil {
			continue
		}
		prop, ok := mappings.GetProperty(key)
		if !ok || !prop.Index {
			continue
		}
		if prop.Type == "vector" {
			values, ok := value.([]interface{})
			if !ok || len(values) != prop.Dims {
				return nil, nil, fmt.Errorf("invalid prepared vector field %s", key)
			}
			vector := make([]float32, len(values))
			for i, value := range values {
				number, ok := value.(float64)
				if !ok {
					return nil, nil, fmt.Errorf("invalid prepared vector field %s at %d", key, i)
				}
				vector[i] = float32(number)
			}
			vectors[key] = vector
			continue
		}
		if values, ok := value.([]interface{}); ok {
			for _, value := range values {
				if err := buildField(mappings, analyzers, bdoc, key, value); err != nil {
					return nil, nil, err
				}
			}
		} else if err := buildField(mappings, analyzers, bdoc, key, value); err != nil {
			return nil, nil, err
		}
	}
	source, err := json.Marshal(doc.Source)
	if err != nil {
		return nil, nil, fmt.Errorf("serialize document %s source: %w", docID, err)
	}
	bdoc.AddField(bluge.NewStoredOnlyField("_source", source))
	bdoc.AddField(bluge.NewStoredOnlyField("_index", []byte(indexName)))
	bdoc.SetTimestamp(doc.Timestamp)
	return bdoc, vectors, nil
}

func buildField(mappings *meta.Mappings, analyzers map[string]*analysis.Analyzer, bdoc *bluge.Document, key string, value interface{}) error {
	prop, _ := mappings.GetProperty(key)
	var field *bluge.TermField
	switch prop.Type {
	case "text", "keyword":
		v, ok := value.(string)
		if !ok {
			return fmt.Errorf("invalid prepared string field %s", key)
		}
		if v == "" {
			return nil
		}
		if prop.Type == "text" {
			field = bluge.NewTextField(key, v).SearchTermPositions()
			analyzer, _ := zincanalysis.QueryAnalyzerForField(analyzers, mappings, key)
			if analyzer != nil {
				field.WithAnalyzer(analyzer)
			}
		} else {
			field = bluge.NewKeywordField(key, v)
		}
	case "numeric":
		v, ok := value.(float64)
		if !ok {
			return fmt.Errorf("invalid prepared numeric field %s", key)
		}
		field = bluge.NewNumericField(key, v)
	case "bool":
		v, ok := value.(bool)
		if !ok {
			return fmt.Errorf("invalid prepared boolean field %s", key)
		}
		field = bluge.NewKeywordField(key, strconv.FormatBool(v))
	case "date", "time":
		v, err := zutils.ParseTime(value, prop.Format, prop.TimeZone)
		if err != nil {
			return fmt.Errorf("field [%s] value [%v] parse err: %w", key, value, err)
		}
		field = bluge.NewDateTimeField(key, v)
	default:
		return fmt.Errorf("field [%s] has unsupported type [%s]", key, prop.Type)
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
	for subfield := range prop.Fields {
		if err := buildField(mappings, analyzers, bdoc, key+"."+subfield, value); err != nil {
			return err
		}
	}
	return nil
}
