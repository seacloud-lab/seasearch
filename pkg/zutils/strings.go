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

package zutils

import (
	"crypto/md5"
	"strconv"
	"strings"
	"unicode"

	"github.com/zincsearch/zincsearch/pkg/zutils/base62"
)

func StringToInt(s string) int {
	i, _ := strconv.Atoi(strings.TrimSpace(s))
	return i
}

func IsNumeric(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// SliceExists checks if a string is in a set.
func SliceExists(set []string, find string) bool {
	for _, s := range set {
		if s == find {
			return true
		}
	}
	return false
}

func GetHashEncode(str string) string {
	sum := md5.Sum([]byte(str))
	return base62.EncodeToString(sum[:], 22) // for md5, the encode result length is 22
}
