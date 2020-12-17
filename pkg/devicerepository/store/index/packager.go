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
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/blevesearch/bleve"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store/remote"
	"go.thethings.network/lorawan-stack/v3/pkg/errors"
	"go.thethings.network/lorawan-stack/v3/pkg/fetch"
	"go.thethings.network/lorawan-stack/v3/pkg/jsonpb"
	"go.thethings.network/lorawan-stack/v3/pkg/log"
	"go.thethings.network/lorawan-stack/v3/pkg/ttnpb"
)

type indexableBrand struct {
	BrandPB  string // *ttnpb.EndDeviceBrand marshaled into JSON string
	ModelsPB string // []*ttnpb.EndDeviceModel marshaled into JSON string

	// Stored in separate fields to support ordering search results
	BrandID   string
	BrandName string
}

type indexableModel struct {
	BrandPB string // *ttnpb.EndDeviceBrand marshaled into JSON string
	ModelPB string // *ttnpb.EndDeviceModel marshaled into JSON string

	// Stored in separate fields to support ordering search results
	BrandID   string
	BrandName string
	ModelID   string
	ModelName string
}

const (
	brandsIndexPath = "brandsIndex.bleve"
	modelsIndexPath = "modelsIndex.bleve"
)

func newIndex(path string, overwrite bool) (bleve.Index, error) {
	mapping := bleve.NewIndexMapping()
	if st, err := os.Stat(path); err == nil && st.IsDir() && overwrite {
		if err := os.RemoveAll(path); err != nil {
			return nil, err
		}
	}
	return bleve.New(path, mapping)
}

// CreatePackage creates a new package usable by the Device Repository.
func CreatePackage(ctx context.Context, f fetch.Interface, workingDirectory, destinationFile string, overwrite bool) error {
	s := remote.NewRemoteStore(f)

	workingDirectory = strings.TrimRight(workingDirectory, "/")
	if err := os.MkdirAll(workingDirectory, 0755); err != nil {
		return err
	}

	brandsIndex, err := newIndex(path.Join(workingDirectory, brandsIndexPath), overwrite)
	if err != nil {
		return err
	}
	modelsIndex, err := newIndex(path.Join(workingDirectory, modelsIndexPath), overwrite)
	if err != nil {
		return err
	}

	brands, err := s.GetBrands(store.GetBrandsRequest{
		Paths: ttnpb.EndDeviceBrandFieldPathsNested,
	})
	if err != nil {
		return err
	}

	brandsBatch := brandsIndex.NewBatch()
	modelsBatch := modelsIndex.NewBatch()
	for _, brand := range brands.Brands {
		log.FromContext(ctx).WithField("brand_id", brand.BrandID).Debug("Indexing")
		models, err := s.GetModels(store.GetModelsRequest{
			Paths:   ttnpb.EndDeviceModelFieldPathsNested,
			BrandID: brand.BrandID,
		})
		if errors.IsNotFound(err) {
			// Skip vendors without any models
			continue
		} else if err != nil {
			return err
		}
		brandPB, err := jsonpb.TTN().Marshal(brand)
		if err != nil {
			return err
		}
		modelsPB, err := jsonpb.TTN().Marshal(models.Models)
		if err != nil {
			return err
		}
		if err := brandsBatch.Index(brand.BrandID, indexableBrand{
			BrandPB:   string(brandPB),
			ModelsPB:  string(modelsPB),
			BrandID:   brand.BrandID,
			BrandName: brand.Name,
		}); err != nil {
			return err
		}
		for _, model := range models.Models {
			modelPB, err := jsonpb.TTN().Marshal(model)
			if err != nil {
				return err
			}
			if err := modelsBatch.Index(fmt.Sprintf("%s/%s", brand.BrandID, model.ModelID), indexableModel{
				BrandPB:   string(brandPB),
				ModelPB:   string(modelPB),
				BrandID:   brand.BrandID,
				BrandName: brand.Name,
				ModelID:   model.ModelID,
				ModelName: model.Name,
			}); err != nil {
				return err
			}
		}
	}
	if err := brandsIndex.Batch(brandsBatch); err != nil {
		return err
	}
	if err := modelsIndex.Batch(modelsBatch); err != nil {
		return err
	}

	// archive working directory, keeping only yaml, js and index files.
	return (&archiver{}).Archive(workingDirectory, destinationFile, func(path string) (string, bool) {
		p := path[len(workingDirectory)+1:]
		if !strings.HasPrefix(p, brandsIndexPath) &&
			!strings.HasPrefix(p, modelsIndexPath) &&
			!(strings.HasPrefix(p, "vendor") && (strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".js"))) {
			return "", false
		}
		return p, true
	})
}
