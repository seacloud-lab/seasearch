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
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
	"github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/meta"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

func MatchQuery(query map[string]interface{}, mappings *meta.Mappings, analyzers map[string]*analysis.Analyzer) (bluge.Query, error) {
	if len(query) > 1 {
		return nil, errors.New(errors.ErrorTypeParsingException, "[match] query doesn't support multiple fields")
	}

	field := ""
	value := new(meta.MatchQuery)
	value.Boost = -1.0
	var minimumShouldMatch interface{}
	for k, v := range query {
		field = k
		switch v := v.(type) {
		case string:
			value.Query = v
		case map[string]interface{}:
			for k, v := range v {
				k := strings.ToLower(k)
				switch k {
				case "query":
					value.Query, _ = zutils.ToString(v)
				case "analyzer":
					value.Analyzer, _ = zutils.ToString(v)
				case "operator":
					value.Operator, _ = zutils.ToString(v)
				case "fuzziness":
					value.Fuzziness = v
				case "prefix_length":
					value.PrefixLength, _ = zutils.ToFloat64(v)
				case "boost":
					value.Boost, _ = zutils.ToFloat64(v)
				case "minimum_should_match":
					minimumShouldMatch = v
				default:
					// return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[match] unknown field [%s]", k))
				}
			}
		default:
			return nil, errors.New(errors.ErrorTypeXContentParseException, fmt.Sprintf("[match] %s doesn't support values of type: %T", k, v))
		}
	}

	var err error
	var zer *analysis.Analyzer
	if value.Analyzer != "" {
		zer, err = zincanalysis.QueryAnalyzer(analyzers, value.Analyzer)
		if err != nil {
			return nil, err
		}
	} else {
		indexZer, searchZer := zincanalysis.QueryAnalyzerForField(analyzers, mappings, field)
		if zer == nil && searchZer != nil {
			zer = searchZer
		}
		if zer == nil && indexZer != nil {
			zer = indexZer
		}
	}

	// only "OR" supports minimum should match
	if minimumShouldMatch != nil && (value.Operator == "" || strings.ToUpper(value.Operator) == "OR") {
		return genQueryWithMinimumShouldMatch(zer, field, value, minimumShouldMatch)
	}

	subq := bluge.NewMatchQuery(value.Query).SetField(field)
	if zer != nil {
		subq.SetAnalyzer(zer)
	}
	if value.Operator != "" {
		op := strings.ToUpper(value.Operator)
		switch op {
		case "OR":
			subq.SetOperator(bluge.MatchQueryOperatorOr)
		case "AND":
			subq.SetOperator(bluge.MatchQueryOperatorAnd)
		default:
			return nil, errors.New(errors.ErrorTypeIllegalArgumentException, fmt.Sprintf("[match] unknown operator %s", op))
		}
	}
	if value.Fuzziness != nil {
		if value.Fuzziness != nil {
			v := ParseFuzziness(value.Fuzziness, value.Query, zer)
			if v > 0 {
				subq.SetFuzziness(v)
			}
		}
	}
	if value.PrefixLength > 0 {
		subq.SetPrefix(int(value.PrefixLength))
	}
	if value.Boost >= 0 {
		subq.SetBoost(value.Boost)
	}
	return subq, err
}

func MatchQueryTerms(query map[string]interface{}, mappings *meta.Mappings, analyzers map[string]*analysis.Analyzer) ([]Term, error) {
	if len(query) > 1 {
		return nil, errors.New(errors.ErrorTypeParsingException, "[match] query doesn't support multiple fields")
	}
	field := ""
	value := new(meta.MatchQuery)

	for k, v := range query {
		field = k
		switch v := v.(type) {
		case string:
			value.Query = v
		case map[string]interface{}:
			for k, v := range v {
				k := strings.ToLower(k)
				switch k {
				case "query":
					value.Query, _ = zutils.ToString(v)
				case "analyzer":
					value.Analyzer, _ = zutils.ToString(v)
				case "fuzziness":
					return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[match] unified search unsupport [%s]", k))
				case "prefix_length":
					return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[match] unified search unsupport [%s]", k))
				default:
					// return nil, errors.New(errors.ErrorTypeParsingException, fmt.Sprintf("[match] unknown field [%s]", k))
				}
			}
		default:
			return nil, errors.New(errors.ErrorTypeXContentParseException, fmt.Sprintf("[match] %s doesn't support values of type: %T", k, v))
		}
	}

	var err error
	var zer *analysis.Analyzer
	if value.Analyzer != "" {
		zer, err = zincanalysis.QueryAnalyzer(analyzers, value.Analyzer)
		if err != nil {
			return nil, err
		}
	} else {
		indexZer, searchZer := zincanalysis.QueryAnalyzerForField(analyzers, mappings, field)
		if zer == nil && searchZer != nil {
			zer = searchZer
		}
		if zer == nil && indexZer != nil {
			zer = indexZer
		}
	}
	if zer == nil {
		zer = analyzer.NewStandardAnalyzer()
	}

	tokens := zer.Analyze([]byte(value.Query))
	if len(tokens) > 0 {
		result := make([]Term, len(tokens))
		for i, tk := range tokens {
			result[i] = NewTerm(field, tk.Term)
		}
		return result, nil
	}
	return nil, nil
}

func genQueryWithMinimumShouldMatch(ana *analysis.Analyzer, field string, value *meta.MatchQuery, minimumShouldMatch interface{}) (bluge.Query, error) {
	if ana == nil {
		ana = analyzer.NewStandardAnalyzer()
	}

	var fuzziness int
	if value.Fuzziness != nil {
		if value.Fuzziness != nil {
			v := ParseFuzziness(value.Fuzziness, value.Query, ana)
			if v > 0 {
				fuzziness = v
			}
		}
	}

	var boost float64 = 1
	if value.Boost >= 0 {
		boost = value.Boost
	}

	tokens := ana.Analyze([]byte(value.Query))
	if len(tokens) > 0 {
		tqs := make([]bluge.Query, len(tokens))
		if fuzziness != 0 {
			for i, token := range tokens {
				query := bluge.NewFuzzyQuery(string(token.Term))
				query.SetFuzziness(fuzziness)
				query.SetPrefix(int(value.PrefixLength))
				query.SetField(field)
				query.SetBoost(boost)
				tqs[i] = query
			}
		} else {
			for i, token := range tokens {
				tq := bluge.NewTermQuery(string(token.Term))
				tq.SetField(field)
				tq.SetBoost(boost)
				tqs[i] = tq
			}
		}
		minValue, err := calculateMin(len(tokens), minimumShouldMatch)
		if err != nil {
			return nil, err
		}
		booleanQuery := bluge.NewBooleanQuery()
		booleanQuery.AddShould(tqs...)
		booleanQuery.SetMinShould(minValue)
		booleanQuery.SetBoost(boost)
		return booleanQuery, nil
	}
	return bluge.NewMatchNoneQuery(), nil
}

type combination struct {
	condition int
	count     int
}

var regex = regexp.MustCompile(`(\d+)<([-+]?\d+%?)`)

// calculateMin
// calculate the MinimumShouldMatch value with given expr and sub query count.
func calculateMin(subCount int, v interface{}) (res int, err error) {
	if subCount == 0 {
		return 1, nil
	}
	defer func() {
		if err != nil {
			return
		}
		if res <= 1 {
			res = 1
			return
		}
		if res >= subCount {
			res = subCount
			return
		}
	}()
	switch x := v.(type) {
	case int64, int, float64:
		m := 0
		switch val := v.(type) {
		case int:
			m = val
		case int64:
			m = int(val)
		case float64:
			m = int(math.Floor(val))
		}
		if m < 0 {
			m = subCount + m
		}
		return m, nil
	case []string:
		conditions := make([]combination, len(x))
		for i, str := range x {
			match := regex.FindStringSubmatch(str)
			if match != nil {
				leftPart := match[1]
				rightPart := match[2]
				condition, err := strconv.ParseInt(leftPart, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("cannot parse the condition value: %w", err)
				}
				count, err := getPartValue(subCount, rightPart)
				if err != nil {
					return 0, fmt.Errorf("cannot parse the clauses count: %w", err)
				}
				conditions[i] = combination{
					condition: int(condition),
					count:     count,
				}
			} else {
				return 0, fmt.Errorf("invalid MinimumShould value: %s", x)
			}
		}

		sort.Slice(conditions, func(i, j int) bool {
			return conditions[i].condition < conditions[j].condition
		})

		for i, condition := range conditions {
			// only match first
			if subCount <= condition.condition {
				return subCount, nil
			}
			// we are the last one, matched
			if i == len(conditions)-1 {
				return condition.count, nil
			}
			// less than next, we matched
			if subCount <= conditions[i+1].condition {
				return condition.count, nil
			}
		}
		return 0, fmt.Errorf("invalid MinimumShould value: %v", x)
	case string:
		combinations := strings.Split(x, " ")
		if len(combinations) > 1 {
			return calculateMin(subCount, combinations)
		}
		// simple expr
		if res, err := getPartValue(subCount, x); err == nil {
			return res, nil
		}

		// complex, we use regex
		match := regex.FindStringSubmatch(x)
		if match != nil {
			leftPart := match[1]
			rightPart := match[2]
			condition, err := strconv.ParseInt(leftPart, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("cannot parse the condition value: %w", err)
			}
			count, err := getPartValue(subCount, rightPart)
			if err != nil {
				return 0, fmt.Errorf("cannot parse the clauses count: %w", err)
			}
			if subCount <= int(condition) {
				return subCount, nil
			}
			return count, nil
		} else {
			return 0, fmt.Errorf("invalid MinimumShould value: %s", x)
		}
	default:
		return 0, fmt.Errorf("invalid MinimumShouldMatch value")
	}
}

func getPartValue(termCount int, part string) (int, error) {
	if strings.Contains(part, "%") {
		proportion, err := strconv.ParseInt(part[0:len(part)-1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse a percent value: %w", err)
		}
		if proportion < 0 {
			count := float64(termCount) * float64(-proportion) / 100
			return termCount - int(count), nil
		}
		count := float64(termCount) * float64(proportion) / 100
		return int(count), nil
	}
	count, err := strconv.ParseInt(part, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse a int value: %w", err)
	}
	if count < 0 {
		return termCount + int(count), nil
	}
	return int(count), nil
}
