// Copyright © 2020 The Things Network Foundation, The Things Industries B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"context"
	"path"

	"github.com/blevesearch/bleve"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store/remote"
	"go.thethings.network/lorawan-stack/v3/pkg/errors"
	"go.thethings.network/lorawan-stack/v3/pkg/fetch"
)

// indexStore wraps a store.Store adding support for searching/sorting results using a bleve index.
type indexStore struct {
	ctx context.Context

	store store.Store

	brandsIndex bleve.Index
	modelsIndex bleve.Index
}

var (
	errNoWorkingDirectory = errors.DefineInvalidArgument("no_working_directory", "no working directory specified")
	errNoFetcherConfig    = errors.DefineInvalidArgument("no_fetcher_config", "no index fetcher configuration specified")
)

// NewStore returns a new indexStore from configuration.
func NewStore(ctx context.Context, f fetch.Interface, workingDirectory string) (store.Store, error) {
	if workingDirectory == "" {
		return nil, errNoWorkingDirectory.New()
	}
	if f == nil {
		return nil, errNoFetcherConfig.New()
	}

	b, err := f.File("package.zip")
	if err != nil {
		return nil, err
	}
	if err := (&archiver{}).Unarchive(b, workingDirectory); err != nil {
		return nil, err
	}

	s := &indexStore{
		ctx: ctx,

		store: remote.NewRemoteStore(fetch.FromFilesystem(workingDirectory)),
	}

	s.brandsIndex, err = bleve.Open(path.Join(workingDirectory, brandsIndexPath))
	if err != nil {
		return nil, err
	}
	s.modelsIndex, err = bleve.Open(path.Join(workingDirectory, modelsIndexPath))
	if err != nil {
		return nil, err
	}

	go func() {
		select {
		case <-s.ctx.Done():
			s.modelsIndex.Close()
			s.brandsIndex.Close()
		}
	}()

	return s, nil
}
