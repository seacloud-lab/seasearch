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

package metadata

import (
	"regexp"
	"sort"
	"strings"

	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

type template struct{}

var Template = new(template)

// Match selects the highest-priority matching template using the existing index
// pattern rules. Callers must copy its mutable configuration before applying it.
func (t *template) Match(indexName string) (*meta.IndexTemplate, error) {
	templates, err := t.List(0, 0)
	if err != nil {
		return nil, err
	}
	sort.Slice(templates, func(i, j int) bool {
		return templates[i].IndexTemplate.Priority > templates[j].IndexTemplate.Priority
	})
	var filteredTemplates []*meta.Template
	for _, tpl := range templates {
		for _, pattern := range tpl.IndexTemplate.IndexPatterns {
			if strings.HasPrefix(indexName, pattern[:1]) {
				filteredTemplates = append(filteredTemplates, tpl)
				break
			}
		}
	}
	for _, tpl := range filteredTemplates {
		for _, pattern := range tpl.IndexTemplate.IndexPatterns {
			pattern := strings.TrimRight(strings.ReplaceAll(pattern, "*", ".*"), "$") + "$"
			if regexp.MustCompile(pattern).MatchString(indexName) {
				return tpl.IndexTemplate, nil
			}
		}
	}
	return nil, nil
}

func (t *template) List(offset, limit int) ([]*meta.Template, error) {
	data, err := db.List(t.key(""), offset, limit)
	if err != nil {
		return nil, err
	}
	templates := make([]*meta.Template, 0, len(data))
	for _, d := range data {
		tpl := new(meta.Template)
		err = json.Unmarshal(d, tpl)
		if err != nil {
			return nil, err
		}
		templates = append(templates, tpl)
	}
	return templates, nil
}

func (t *template) Get(id string) (*meta.Template, error) {
	data, err := db.Get(t.key(id))
	if err != nil {
		return nil, err
	}
	tpl := new(meta.Template)
	err = json.Unmarshal(data, tpl)
	return tpl, err
}

func (t *template) Set(id string, val meta.Template) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return db.Set(t.key(id), data)
}

func (t *template) Delete(id string) error {
	return db.Delete(t.key(id))
}

func (t *template) key(id string) string {
	return "/template/" + id
}
