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
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core/vector"
	"github.com/zincsearch/zincsearch/pkg/metadata"
)

func DeleteIndex(name string) error {
	// 1. Check if index exists
	index, exists := GetIndex(name)
	if !exists {
		return errors.New("index " + name + " does not exists")
	}
	// delete vecIndexes
	vecIndexes := index.GetVecIndexes()
	for vecIndex := range vecIndexes {
		err := DeleteVectorIndex(name, vecIndex)
		if err != nil {
			return fmt.Errorf("delete vec index err: %w", err)
		}
	}
	// remove the parent dir containing all vec indexes for this index.
	err := os.RemoveAll(path.Join(config.Global.DataPath, vector.VecPrefix, index.GetStoreName()))
	if err != nil {
		return fmt.Errorf("delete vec index err: %w", err)
	}

	// 2. Physically delete the index
	err = delIndex(index)
	if err != nil {
		return fmt.Errorf("delete index err: %w", err)
	}

	// 3. Close and Delete from cache
	ZINC_INDEX_LIST.Delete(name)
	// 4. Delete form metadata
	return metadata.Index.Delete(name)
}

func delIndex(index *Index) error {
	switch index.ref.StorageType {
	case "oss":
		return directory.RemoveOssIndex(index.GetStoreName())
	case "s3":
		return directory.RemoveS3Index(index.GetStoreName())
	default:
		dataPath := config.Global.DataPath
		err := os.RemoveAll(dataPath + "/" + index.GetStoreName())
		if err != nil {
			return fmt.Errorf("failed to delete index: %w", err)
		}
		return nil
	}
}
