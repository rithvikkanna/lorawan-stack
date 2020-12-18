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
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store/remote"
	"go.thethings.network/lorawan-stack/v3/pkg/errors"
	"go.thethings.network/lorawan-stack/v3/pkg/fetch"
	"go.thethings.network/lorawan-stack/v3/pkg/log"
)

// indexStore wraps a store.Store adding support for searching/sorting results using a bleve index.
type indexStore struct {
	ctx context.Context

	store   store.Store
	storeMu sync.RWMutex

	brandsIndex   bleve.Index
	brandsIndexMu sync.RWMutex
	modelsIndex   bleve.Index
	modelsIndexMu sync.RWMutex

	workingDirectory string

	fetcher   fetch.Interface
	refreshCh <-chan time.Time
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

	s := &indexStore{
		ctx: ctx,

		store:            remote.NewRemoteStore(fetch.FromFilesystem(workingDirectory)),
		workingDirectory: workingDirectory,
		fetcher:          f,
	}

	if err := s.initStore(); err != nil {
		return nil, err
	}

	go func() {
		select {
		case <-s.ctx.Done():
			s.modelsIndex.Close()
			s.brandsIndex.Close()
		}
	}()

	s.refreshCh = time.NewTicker(time.Hour).C
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-s.refreshCh:
				logger := log.FromContext(ctx)

				logger.Debug("Refreshing Device Repository")
				if err := s.initStore(); err != nil {
					logger.WithError(err).Error("Failed to refresh device repository")
				} else {
					logger.Info("Updated Device Repository")
				}
			}
		}
	}()

	return s, nil
}

func (s *indexStore) initStore() error {
	b, err := s.fetcher.File("package.zip")
	if err != nil {
		return err
	}

	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	if err := (&archiver{}).Unarchive(b, s.workingDirectory); err != nil {
		return err
	}

	s.brandsIndexMu.Lock()
	defer s.brandsIndexMu.Unlock()
	if s.brandsIndex != nil {
		if err := s.brandsIndex.Close(); err != nil {
			return err
		}
	}
	s.brandsIndex, err = bleve.Open(path.Join(s.workingDirectory, brandsIndexPath))
	if err != nil {
		return err
	}
	s.modelsIndexMu.Lock()
	defer s.modelsIndexMu.Unlock()
	if s.modelsIndex != nil {
		if err := s.modelsIndex.Close(); err != nil {
			return err
		}
	}
	s.modelsIndex, err = bleve.Open(path.Join(s.workingDirectory, modelsIndexPath))
	if err != nil {
		return err
	}
	return nil
}
