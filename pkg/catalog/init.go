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

package catalog

import (
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"

	"github.com/rs/zerolog/log"
)

// Init initializes persisted version and aliases without runtime workers.
func Init() {
	// check version
	version, _ := metadata.KV.Get("version")
	if version == nil {
		// version have version from v0.2.5
		// so if no version, it should be <= v0.2.4
		version = []byte("v0.2.4")
	}

	// update version
	if string(version) != meta.Version {
		err := metadata.KV.Set("version", []byte(meta.Version))
		if err != nil {
			log.Error().Err(err).Msg("Error set version")
		}
	}

	var err error
	ZINC_INDEX_ALIAS_LIST.Aliases, err = metadata.Alias.Get()
	if err != nil {
		log.Fatal().Err(err).Msg("Error loading alias")
	}
}
