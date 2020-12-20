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

package devicerepository

import (
	"context"

	"go.thethings.network/lorawan-stack/v3/pkg/config"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store/bleve"
	"go.thethings.network/lorawan-stack/v3/pkg/fetch"
)

// Config represents the DeviceRepository configuration.
type Config struct {
	Store StoreConfig `name:"store"`

	ConfigSource string                `name:"config-source" description:"Source of the device repository (static, directory, url, blob)"`
	Static       map[string][]byte     `name:"-"`
	Directory    string                `name:"directory" description:"OS filesystem directory, which contains device repository package"`
	URL          string                `name:"url" description:"URL, which contains device repository package"`
	Blob         config.BlobPathConfig `name:"blob"`

	Bleve bleve.Config `name:"bleve"`

	AssetsBaseURL string `name:"assets-base-url" description:"The base URL for assets"`
}

// StoreConfig represents configuration for the Device Repository store.
type StoreConfig struct {
	Store store.Store `name:"-"`
}

// NewStore creates a new Store for end devices.
func (c Config) NewStore(ctx context.Context, blobConf config.BlobConfig) (store.Store, error) {
	if c.Store.Store != nil {
		return c.Store.Store, nil
	}
	var fetcher fetch.Interface
	switch c.ConfigSource {
	case "static":
		fetcher = fetch.NewMemFetcher(c.Static)
	case "directory":
		fetcher = fetch.FromFilesystem(c.Directory)
	case "url":
		var err error
		fetcher, err = fetch.FromHTTP(c.URL, true)
		if err != nil {
			return nil, err
		}
	case "blob":
		b, err := blobConf.Bucket(ctx, c.Blob.Bucket)
		if err != nil {
			return nil, err
		}
		fetcher = fetch.FromBucket(ctx, b, c.Blob.Path)
	default:
		return &store.NoopStore{}, nil
	}

	s, err := c.Bleve.NewStore(ctx, fetcher)
	if err != nil {
		return nil, err
	}
	return s, nil
}
