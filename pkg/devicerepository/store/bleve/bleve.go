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

package bleve

import (
	"context"
	"os"
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

// defaultTimeout is the timeout while trying to open the index. This is to avoid
// blocking on the index open call, which will hung indefinetetly if the index
// is already in use by a different process.
var defaultTimeout = 5 * time.Second

// packageFileName is the name of the Device Repository package.
const packageFileName = "package.zip"

// bleveStore wraps a store.Store adding support for searching/sorting results using a bleve index.
type bleveStore struct {
	ctx context.Context

	store   store.Store
	storeMu sync.RWMutex

	indexMu     sync.RWMutex
	brandsIndex bleve.Index
	modelsIndex bleve.Index

	workingDirectory string

	fetcher   fetch.Interface
	refreshCh <-chan time.Time
}

var errNoFetcherConfig = errors.DefineInvalidArgument("no_fetcher_config", "no index fetcher configuration specified")

// NewStore returns a new Device Repository store with indexing capabilities (using bleve).
func (c Config) NewStore(ctx context.Context, f fetch.Interface) (store.Store, error) {
	if c.WorkingDirectory == "" {
		var err error
		c.WorkingDirectory, err = os.Getwd()
		if err != nil {
			return nil, err
		}
	}
	if f == nil {
		return nil, errNoFetcherConfig.New()
	}

	s := &bleveStore{
		ctx: ctx,

		store:   remote.NewRemoteStore(fetch.FromFilesystem(c.WorkingDirectory)),
		fetcher: f,

		workingDirectory: c.WorkingDirectory,
	}

	if err := s.openStore(c.AutoInit); err != nil {
		return nil, err
	}

	go func() {
		<-s.ctx.Done()
		if s.modelsIndex != nil {
			s.modelsIndex.Close()
		}
		if s.brandsIndex != nil {
			s.brandsIndex.Close()
		}
	}()

	if d := c.RefreshInterval; d > 0 {
		s.refreshCh = time.NewTicker(d).C
		go func() {
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-s.refreshCh:
					logger := log.FromContext(ctx)

					logger.Debug("Refreshing Device Repository")
					if err := s.openStore(true); err != nil {
						logger.WithError(err).Error("Failed to refresh Device Repository")
					} else {
						logger.Info("Updated Device Repository")
					}
				}
			}
		}()
	}

	return s, nil
}

func (s *bleveStore) openIndex(ctx context.Context, path string) (bleve.Index, error) {
	var (
		err   error
		index bleve.Index
	)
	done := make(chan struct{}, 1)
	defer close(done)
	go func() {
		index, err = bleve.Open(path)
		done <- struct{}{}
	}()
	select {
	case <-done:
		return index, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *bleveStore) openStore(withFetch bool) error {
	var err error
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	if s.brandsIndex != nil {
		if err := s.brandsIndex.Close(); err != nil {
			return err
		}
	}
	if s.modelsIndex != nil {
		if err := s.modelsIndex.Close(); err != nil {
			return err
		}
	}
	if withFetch {
		b, err := s.fetcher.File(packageFileName)
		if err != nil {
			return err
		}

		// Lock templates and payload formatters for as little as possible
		s.storeMu.Lock()
		err = unarchive(b, s.workingDirectory)
		s.storeMu.Unlock()
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(s.ctx, defaultTimeout)
	defer cancel()
	s.brandsIndex, err = s.openIndex(ctx, path.Join(s.workingDirectory, brandsIndexPath))
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(s.ctx, defaultTimeout)
	defer cancel()
	s.modelsIndex, err = s.openIndex(ctx, path.Join(s.workingDirectory, modelsIndexPath))
	if err != nil {
		return err
	}
	return nil
}
