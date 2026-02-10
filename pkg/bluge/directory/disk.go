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

package directory

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/rs/zerolog/log"
)

// GetDiskConfig returns a bluge config that will store index data in local disk
// rootPath: the root path of data
// indexName: the name of the index to use.
func GetDiskConfig(rootPath string, indexName string, timeRange ...int64) bluge.Config {
	config := index.DefaultConfig(path.Join(rootPath, indexName))

	config.IndexName = indexName
	config = config.WithPersisterNapTimeMSec(50)
	// ConcurrentSegmentLoad defaults to 1 for local disk, since disk I/O
	// typically supports limited concurrency compared to network I/O.

	config.DirectoryFunc = func() index.Directory {
		fs := index.NewFileSystemDirectory(path.Join(rootPath, indexName))
		return &fileSystemDirectory{
			FileSystemDirectory: *fs,
			path:                path.Join(rootPath, indexName),
		}
	}

	if len(timeRange) == 2 {
		if timeRange[0] <= timeRange[1] {
			config = config.WithTimeRange(timeRange[0], timeRange[1])
		}
	}

	return bluge.DefaultConfigWithIndexConfig(config)
}

// fileSystemDirectory
// The locking operation is not necessary for us.
// In the case of loading many indexes, many lock operations will take more time.
type fileSystemDirectory struct {
	index.FileSystemDirectory
	path string
}

func (d *fileSystemDirectory) Lock() error {
	return nil
}

func (d *fileSystemDirectory) Unlock() error {
	return nil
}

var ErrPathNotExists = errors.New("setup err: readOnly, directory does not exist")

func (d *fileSystemDirectory) Setup(readOnly bool) error {
	items, err := os.ReadDir(d.path)
	if os.IsNotExist(err) {
		if readOnly {
			return ErrPathNotExists
		}
		err = os.MkdirAll(d.path, 0777)
		if err != nil {
			log.Error().Err(err).Msgf("Setup %s err: create dir err: ", d.path)
			return fmt.Errorf("setup err: error creating directory '%s': %w", d.path, err)
		}
	} else if err != nil {
		log.Error().Err(err).Msgf("Setup %s err: read dir error: ", d.path)
		return fmt.Errorf("setup err: read dir error '%s': %w", d.path, err)
	} else if len(items) == 0 {
		if readOnly {
			return ErrPathNotExists
		}
	}
	return nil
}
