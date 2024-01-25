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

package query

import (
	"fmt"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"

	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

func Query(query interface{}, mappings *meta.Mappings, analyzers map[string]*analysis.Analyzer) (bluge.Query, error) {
	if query == nil {
		return MatchAllQuery()
	}

	if q, ok := query.(*meta.Query); ok {
		data, err := json.Marshal(q)
		if err != nil {
			return nil, errors.New(errors.ErrorTypeInvalidArgument, "query must be a map[string]interface{}")
		}
		var newQuery map[string]interface{}
		if err = json.Unmarshal(data, &newQuery); err != nil {
			return nil, errors.New(errors.ErrorTypeInvalidArgument, "query must be a map[string]interface{}")
		}
		query = newQuery
	}
	q, ok := query.(map[string]interface{})
	if !ok {
		return nil, errors.New(errors.ErrorTypeInvalidArgument, "query must be a map[string]interface{}")
	}

	var subq bluge.Query
	var cmd string
	var err error
	for k, t := range q {
		if subq != nil && cmd != "" {
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[%s] malformed query, excepted [END_OBJECT] but found [FIELD_NAME] %s", cmd, k))
		}
		k := strings.ToLower(k)
		cmd = k
		v, ok := t.(map[string]interface{})
		if !ok {
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[%s] query doesn't support value type %T", k, t))
		}
		switch k {
		case "bool":
			if subq, err = BoolQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[bool] failed to parse field").Cause(err)
			}
		case "boosting":
			if subq, err = BoostingQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[boosting] failed to parse field").Cause(err)
			}
		case "match":
			if subq, err = MatchQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match] failed to parse field").Cause(err)
			}
		case "match_bool_prefix":
			if subq, err = MatchBoolPrefixQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_bool_prefix] failed to parse field").Cause(err)
			}
		case "match_phrase":
			if subq, err = MatchPhraseQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_phrase] failed to parse field").Cause(err)
			}
		case "match_phrase_prefix":
			if subq, err = MatchPhrasePrefixQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_phrase_prefix] failed to parse field").Cause(err)
			}
		case "multi_match":
			if subq, err = MultiMatchQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[multi_match] failed to parse field").Cause(err)
			}
		case "match_all":
			if subq, err = MatchAllQuery(); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_all] failed to parse field").Cause(err)
			}
		case "match_none":
			if subq, err = MatchNoneQuery(); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_none] failed to parse field").Cause(err)
			}
		case "combined_fields":
			if subq, err = CombinedFieldsQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[combined_fields] failed to parse field").Cause(err)
			}
		case "query_string":
			if subq, err = QueryStringQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[query_string] failed to parse field").Cause(err)
			}
		case "simple_query_string":
			if subq, err = SimpleQueryStringQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[simple_query_string] failed to parse field").Cause(err)
			}
		case "exists":
			if subq, err = ExistsQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[exists] failed to parse field").Cause(err)
			}
		case "ids":
			if subq, err = IdsQuery(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[ids] failed to parse field").Cause(err)
			}
		case "range":
			if subq, err = RangeQuery(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[range] failed to parse field").Cause(err)
			}
		case "regexp":
			if subq, err = RegexpQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[regexp] failed to parse field").Cause(err)
			}
		case "prefix":
			if subq, err = PrefixQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[prefix] failed to parse field").Cause(err)
			}
		case "fuzzy":
			if subq, err = FuzzyQuery(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[fuzzy] failed to parse field").Cause(err)
			}
		case "wildcard":
			if subq, err = WildcardQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[wildcard] failed to parse field").Cause(err)
			}
		case "term":
			if subq, err = TermQuery(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[term] failed to parse field").Cause(err)
			}
		case "terms":
			if subq, err = TermsQuery(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[terms] failed to parse field").Cause(err)
			}
		case "terms_set":
			if subq, err = TermsSetQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[terms_set] failed to parse field").Cause(err)
			}
		case "geo_bounding_box":
			if subq, err = GeoBoundingBoxQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_bounding_box] failed to parse field").Cause(err)
			}
		case "geo_distance":
			if subq, err = GeoDistanceQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_distance] failed to parse field").Cause(err)
			}
		case "geo_polygon":
			if subq, err = GeoPolygonQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_polygon] failed to parse field").Cause(err)
			}
		case "geo_shape":
			if subq, err = GeoShapeQuery(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_shape] failed to parse field").Cause(err)
			}
		default:
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[%s] query doesn't support", k))
		}
	}

	return subq, nil
}

type Term struct {
	Field string
	Value []byte
}

func NewTerm(field string, val []byte) Term {
	return Term{
		Field: field,
		Value: val,
	}
}

// QueryTerms
// get all the terms that is used in the query for unified search.
func QueryTerms(query interface{}, mappings *meta.Mappings, analyzers map[string]*analysis.Analyzer) ([]Term, error) {
	if query == nil {
		return nil, nil
	}
	if q, ok := query.(*meta.Query); ok {
		data, err := json.Marshal(q)
		if err != nil {
			return nil, errors.New(errors.ErrorTypeInvalidArgument, "query must be a map[string]interface{}")
		}
		var newQuery map[string]interface{}
		if err = json.Unmarshal(data, &newQuery); err != nil {
			return nil, errors.New(errors.ErrorTypeInvalidArgument, "query must be a map[string]interface{}")
		}
		query = newQuery
	}
	q, ok := query.(map[string]interface{})
	if !ok {
		return nil, errors.New(errors.ErrorTypeInvalidArgument, "query must be a map[string]interface{}")
	}

	var result []Term
	var cmd string
	var err error
	for k, t := range q {
		if len(result) > 0 && cmd != "" {
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[%s] malformed query, excepted [END_OBJECT] but found [FIELD_NAME] %s", cmd, k))
		}
		k := strings.ToLower(k)
		cmd = k
		v, ok := t.(map[string]interface{})
		if !ok {
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[%s] query doesn't support value type %T", k, t))
		}
		switch k {
		case "bool":
			if result, err = BoolQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[bool] failed to parse field").Cause(err)
			}
		case "boosting":
			if result, err = BoostingQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[boosting] failed to parse field").Cause(err)
			}
		case "match":
			if result, err = MatchQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match] failed to parse field").Cause(err)
			}
		case "match_bool_prefix":
			if result, err = MatchBoolPrefixQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_bool_prefix] failed to parse field").Cause(err)
			}
		case "match_phrase":
			if result, err = MatchPhraseQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_phrase] failed to parse field").Cause(err)
			}
		case "match_phrase_prefix":
			if result, err = MatchPhrasePrefixQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_phrase_prefix] failed to parse field").Cause(err)
			}
		case "multi_match":
			if result, err = MultiMatchQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[multi_match] failed to parse field").Cause(err)
			}
		case "match_all":
			if result, err = MatchAllQueryTerms(); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_all] failed to parse field").Cause(err)
			}
		case "match_none":
			if result, err = MatchNoneQueryTerms(); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[match_none] failed to parse field").Cause(err)
			}
		case "combined_fields":
			if result, err = CombinedFieldsQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[combined_fields] failed to parse field").Cause(err)
			}
		case "query_string":
			if result, err = QueryStringQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[query_string] failed to parse field").Cause(err)
			}
		case "simple_query_string":
			if result, err = SimpleQueryStringQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[simple_query_string] failed to parse field").Cause(err)
			}
		case "exists":
			if result, err = ExistsQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[exists] failed to parse field").Cause(err)
			}
		case "ids":
			if result, err = IdsQueryTerms(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[ids] failed to parse field").Cause(err)
			}
		case "range":
			if result, err = RangeQueryTerms(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[range] failed to parse field").Cause(err)
			}
		case "regexp":
			if result, err = RegexQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[regexp] failed to parse field").Cause(err)
			}
		case "prefix":
			if result, err = PrefixQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[prefix] failed to parse field").Cause(err)
			}
		case "fuzzy":
			if result, err = FuzzyQueryTerms(v, mappings, analyzers); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[fuzzy] failed to parse field").Cause(err)
			}
		case "wildcard":
			if result, err = WildcardQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[wildcard] failed to parse field").Cause(err)
			}
		case "term":
			if result, err = TermQueryTerms(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[term] failed to parse field").Cause(err)
			}
		case "terms":
			if result, err = TermsQueryTerms(v, mappings); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[terms] failed to parse field").Cause(err)
			}
		case "terms_set":
			if result, err = TermsSetQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[terms_set] failed to parse field").Cause(err)
			}
		case "geo_bounding_box":
			if result, err = GeoBoundingBoxQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_bounding_box] failed to parse field").Cause(err)
			}
		case "geo_distance":
			if result, err = GeoDistanceQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_distance] failed to parse field").Cause(err)
			}
		case "geo_polygon":
			if result, err = GeoPolygonQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_polygon] failed to parse field").Cause(err)
			}
		case "geo_shape":
			if result, err = GeoShapeQueryTerms(v); err != nil {
				return nil, errors.New(errors.ErrorTypeXContentParseException, "[geo_shape] failed to parse field").Cause(err)
			}
		default:
			return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[%s] query doesn't support", k))
		}
	}

	return result, nil
}
