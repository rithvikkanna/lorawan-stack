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

package devicerepository_test

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/smartystreets/assertions"
	"go.thethings.network/lorawan-stack/v3/pkg/component"
	componenttest "go.thethings.network/lorawan-stack/v3/pkg/component/test"
	"go.thethings.network/lorawan-stack/v3/pkg/config"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store"
	"go.thethings.network/lorawan-stack/v3/pkg/errors"
	"go.thethings.network/lorawan-stack/v3/pkg/ttnpb"
	"go.thethings.network/lorawan-stack/v3/pkg/util/test"
	"go.thethings.network/lorawan-stack/v3/pkg/util/test/assertions/should"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type mockStore struct {
	// last requests
	lastGetBrandsRequest store.GetBrandsRequest
	lastGetModelsRequest store.GetModelsRequest
	lastVersionIDs       *ttnpb.EndDeviceVersionIdentifiers

	// mock responses
	brands   []*ttnpb.EndDeviceBrand
	models   []*ttnpb.EndDeviceModel
	template *ttnpb.EndDeviceTemplate
	uplinkDecoder,
	downlinkDecoder,
	downlinkEncoder *ttnpb.MessagePayloadFormatter

	// mock errors
	err error
}

// GetBrands lists available end device vendors.
func (s *mockStore) GetBrands(req store.GetBrandsRequest) (*store.GetBrandsResponse, error) {
	s.lastGetBrandsRequest = req
	if s.err != nil {
		return nil, s.err
	}
	if s.brands == nil {
		s.brands = []*ttnpb.EndDeviceBrand{}
	}
	return &store.GetBrandsResponse{
		Count:  uint32(len(s.brands)),
		Offset: 0,
		Total:  uint32(len(s.brands)),
		Brands: s.brands,
	}, nil
}

// GetModels lists available end device definitions.
func (s *mockStore) GetModels(req store.GetModelsRequest) (*store.GetModelsResponse, error) {
	s.lastGetModelsRequest = req
	if s.err != nil {
		return nil, s.err
	}
	if s.models == nil {
		s.models = []*ttnpb.EndDeviceModel{}
	}
	return &store.GetModelsResponse{
		Count:  uint32(len(s.models)),
		Offset: 0,
		Total:  uint32(len(s.models)),
		Models: s.models,
	}, nil
}

// GetTemplate retrieves an end device template for an end device definition.
func (s *mockStore) GetTemplate(ids *ttnpb.EndDeviceVersionIdentifiers) (*ttnpb.EndDeviceTemplate, error) {
	s.lastVersionIDs = ids
	return s.template, s.err
}

// GetUplinkDecoder retrieves the codec for decoding uplink messages.
func (s *mockStore) GetUplinkDecoder(ids *ttnpb.EndDeviceVersionIdentifiers) (*ttnpb.MessagePayloadFormatter, error) {
	s.lastVersionIDs = ids
	return s.uplinkDecoder, s.err
}

// GetDownlinkDecoder retrieves the codec for decoding downlink messages.
func (s *mockStore) GetDownlinkDecoder(ids *ttnpb.EndDeviceVersionIdentifiers) (*ttnpb.MessagePayloadFormatter, error) {
	s.lastVersionIDs = ids
	return s.downlinkDecoder, s.err
}

// GetDownlinkEncoder retrieves the codec for encoding downlink messages.
func (s *mockStore) GetDownlinkEncoder(ids *ttnpb.EndDeviceVersionIdentifiers) (*ttnpb.MessagePayloadFormatter, error) {
	s.lastVersionIDs = ids
	return s.downlinkEncoder, s.err
}

func TestGRPC(t *testing.T) {
	ids := &ttnpb.EndDeviceVersionIdentifiers{
		BrandID:         "brand",
		ModelID:         "model",
		FirmwareVersion: "1.0",
		HardwareVersion: "1.0",
		BandID:          "band",
	}

	componentConfig := &component.Config{
		ServiceBase: config.ServiceBase{
			GRPC: config.GRPC{
				Listen:                      ":0",
				AllowInsecureForCredentials: true,
			},
		},
	}
	c := componenttest.NewComponent(t, componentConfig)

	st := &mockStore{}
	conf := &devicerepository.Config{
		Store:         st,
		AssetsBaseURL: "https://assets/",
	}
	dr, err := devicerepository.New(c, conf)
	test.Must(dr, err)

	componenttest.StartComponent(t, c)
	defer c.Close()

	cc := dr.LoopbackConn()
	cl := ttnpb.NewDeviceRepositoryClient(cc)

	t.Run("ListBrands", func(t *testing.T) {
		t.Run("Request", func(t *testing.T) {
			a := assertions.New(t)

			_, err := cl.ListBrands(test.Context(), &ttnpb.ListEndDeviceBrandsRequest{
				Limit:   100,
				Page:    2,
				OrderBy: "brand_id",
				Search:  "query string",
				FieldMask: types.FieldMask{
					Paths: []string{"lora_alliance_vendor_id"},
				},
			})
			a.So(err, should.BeNil)
			a.So(st.lastGetBrandsRequest, should.Resemble, store.GetBrandsRequest{
				BrandID: "",
				Limit:   100,
				Page:    2,
				OrderBy: "brand_id",
				Paths:   []string{"lora_alliance_vendor_id", "brand_id"},
				Search:  "query string",
			})
		})

		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			brands, err := cl.ListBrands(test.Context(), &ttnpb.ListEndDeviceBrandsRequest{})
			a.So(brands, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.brands = []*ttnpb.EndDeviceBrand{
				{
					BrandID:                 "brand1",
					PrivateEnterpriseNumber: 100,
					Logo:                    "item.png",
				},
				{
					BrandID: "brand2",
				},
			}
			st.err = nil

			responseHeaders := metadata.MD{}
			brands, err := cl.ListBrands(test.Context(), &ttnpb.ListEndDeviceBrandsRequest{}, grpc.Header(&responseHeaders))
			a.So(err, should.BeNil)
			a.So(brands, should.Resemble, &ttnpb.ListEndDeviceBrandsResponse{
				Brands: []*ttnpb.EndDeviceBrand{
					{
						BrandID:                 "brand1",
						PrivateEnterpriseNumber: 100,
						Logo:                    "https://assets/vendor/brand1/item.png",
					},
					{
						BrandID: "brand2",
					},
				},
			})

			s := responseHeaders.Get("x-total-count")
			a.So(s, should.Resemble, []string{"2"})
		})
	})

	t.Run("GetBrand", func(t *testing.T) {
		t.Run("Request", func(t *testing.T) {
			a := assertions.New(t)

			_, err := cl.GetBrand(test.Context(), &ttnpb.GetEndDeviceBrandRequest{
				BrandID: "brand1",
				FieldMask: types.FieldMask{
					Paths: []string{"lora_alliance_vendor_id"},
				},
			})
			a.So(err, should.BeNil)
			a.So(st.lastGetBrandsRequest, should.Resemble, store.GetBrandsRequest{
				Limit:   1,
				BrandID: "brand1",
				Paths:   []string{"lora_alliance_vendor_id", "brand_id"},
			})
		})

		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			brands, err := cl.ListBrands(test.Context(), &ttnpb.ListEndDeviceBrandsRequest{})
			a.So(brands, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.brands = []*ttnpb.EndDeviceBrand{
				{
					BrandID:                 "brand1",
					PrivateEnterpriseNumber: 100,
					Logo:                    "item.png",
				},
			}
			st.err = nil

			brand, err := cl.GetBrand(test.Context(), &ttnpb.GetEndDeviceBrandRequest{})
			a.So(err, should.BeNil)
			a.So(brand, should.Resemble, &ttnpb.EndDeviceBrand{
				BrandID:                 "brand1",
				PrivateEnterpriseNumber: 100,
				Logo:                    "https://assets/vendor/brand1/item.png",
			})
		})
	})

	t.Run("ListModels", func(t *testing.T) {
		t.Run("Request", func(t *testing.T) {
			a := assertions.New(t)

			_, err := cl.ListModels(test.Context(), &ttnpb.ListEndDeviceModelsRequest{
				BrandID: "brand1",
				Limit:   100,
				Page:    2,
				OrderBy: "brand_id",
				Search:  "query string",
				FieldMask: types.FieldMask{
					Paths: []string{"firmware_versions"},
				},
			})
			a.So(err, should.BeNil)
			a.So(st.lastGetModelsRequest, should.Resemble, store.GetModelsRequest{
				ModelID: "",
				BrandID: "brand1",
				Limit:   100,
				Page:    2,
				OrderBy: "brand_id",
				Paths:   []string{"firmware_versions", "brand_id", "model_id"},
				Search:  "query string",
			})
		})

		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			res, err := cl.ListModels(test.Context(), &ttnpb.ListEndDeviceModelsRequest{})
			a.So(res, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.models = []*ttnpb.EndDeviceModel{
				{
					BrandID: "brand1",
					ModelID: "model1",
					Photos: &ttnpb.EndDeviceModel_Photos{
						Main:  "a.png",
						Other: []string{"b.png"},
					},
				},
				{
					BrandID: "brand2",
					ModelID: "model2",
				},
			}
			st.err = nil

			responseHeaders := metadata.MD{}
			brands, err := cl.ListModels(test.Context(), &ttnpb.ListEndDeviceModelsRequest{}, grpc.Header(&responseHeaders))
			a.So(err, should.BeNil)
			a.So(brands, should.Resemble, &ttnpb.ListEndDeviceModelsResponse{
				Models: []*ttnpb.EndDeviceModel{
					{
						BrandID: "brand1",
						ModelID: "model1",
						Photos: &ttnpb.EndDeviceModel_Photos{
							Main:  "https://assets/vendor/brand1/a.png",
							Other: []string{"https://assets/vendor/brand1/b.png"},
						},
					},
					{
						BrandID: "brand2",
						ModelID: "model2",
					},
				},
			})

			s := responseHeaders.Get("x-total-count")
			a.So(s, should.Resemble, []string{"2"})
		})
	})

	t.Run("GetModel", func(t *testing.T) {
		t.Run("Request", func(t *testing.T) {
			a := assertions.New(t)

			_, err := cl.GetModel(test.Context(), &ttnpb.GetEndDeviceModelRequest{
				BrandID: "brand1",
				ModelID: "model1",
				FieldMask: types.FieldMask{
					Paths: []string{"firmware_versions"},
				},
			})
			a.So(err, should.BeNil)
			a.So(st.lastGetModelsRequest, should.Resemble, store.GetModelsRequest{
				Limit:   1,
				BrandID: "brand1",
				ModelID: "model1",
				Paths:   []string{"firmware_versions", "brand_id", "model_id"},
			})
		})

		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			models, err := cl.ListModels(test.Context(), &ttnpb.ListEndDeviceModelsRequest{})
			a.So(models, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.models = []*ttnpb.EndDeviceModel{
				{
					BrandID: "brand1",
					ModelID: "model1",
					Photos: &ttnpb.EndDeviceModel_Photos{
						Main:  "a.png",
						Other: []string{"b.png"},
					},
				},
			}
			st.err = nil

			model, err := cl.GetModel(test.Context(), &ttnpb.GetEndDeviceModelRequest{})
			a.So(err, should.BeNil)
			a.So(model, should.Resemble, &ttnpb.EndDeviceModel{
				BrandID: "brand1",
				ModelID: "model1",
				Photos: &ttnpb.EndDeviceModel_Photos{
					Main:  "https://assets/vendor/brand1/a.png",
					Other: []string{"https://assets/vendor/brand1/b.png"},
				},
			})
		})
	})

	t.Run("GetTemplate", func(t *testing.T) {
		st.template = &ttnpb.EndDeviceTemplate{
			EndDevice: ttnpb.EndDevice{
				VersionIDs: ids,
			},
			FieldMask: types.FieldMask{
				Paths: []string{"version_ids"},
			},
		}

		t.Run("Request", func(t *testing.T) {
			a := assertions.New(t)
			_, err := cl.GetTemplate(test.Context(), &ttnpb.GetTemplateRequest{
				VersionIDs: ids,
			})
			a.So(err, should.BeNil)
			a.So(st.lastVersionIDs, should.Resemble, ids)
		})

		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			models, err := cl.GetTemplate(test.Context(), &ttnpb.GetTemplateRequest{
				VersionIDs: ids,
			})
			a.So(models, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.err = nil

			template, err := cl.GetTemplate(test.Context(), &ttnpb.GetTemplateRequest{
				VersionIDs: ids,
			})
			a.So(err, should.BeNil)
			a.So(template, should.Resemble, st.template)
		})
	})

	t.Run("Authenticate", func(t *testing.T) {
		// TODO authenticate with rights
	})

	t.Run("GetUplinkDecoder", func(t *testing.T) {
		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			models, err := cl.GetUplinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a.So(models, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
			a.So(st.lastVersionIDs, should.Resemble, ids)
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.uplinkDecoder = ttnpb.NewPopulatedMessagePayloadFormatter(test.Randy, true)
			st.err = nil

			c, err := cl.GetUplinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a.So(err, should.BeNil)
			a.So(c, should.Resemble, st.uplinkDecoder)
		})

		t.Run("ClusterAuth", func(t *testing.T) {
			conf.RequireAuth = true
			codec, err := cl.GetUplinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a := assertions.New(t)
			a.So(errors.IsUnauthenticated(err), should.BeTrue)
			a.So(codec, should.BeNil)

			_, err = cl.GetUplinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			}, c.WithClusterAuth())
			a.So(err, should.BeNil)

			conf.RequireAuth = false
		})
	})

	t.Run("GetDownlinkDecoder", func(t *testing.T) {
		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			models, err := cl.GetDownlinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a.So(models, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
			a.So(st.lastVersionIDs, should.Resemble, ids)
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.downlinkDecoder = ttnpb.NewPopulatedMessagePayloadFormatter(test.Randy, true)
			st.err = nil

			c, err := cl.GetDownlinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a.So(err, should.BeNil)
			a.So(c, should.Resemble, st.downlinkDecoder)
		})

		t.Run("ClusterAuth", func(t *testing.T) {
			conf.RequireAuth = true
			codec, err := cl.GetDownlinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a := assertions.New(t)
			a.So(errors.IsUnauthenticated(err), should.BeTrue)
			a.So(codec, should.BeNil)

			_, err = cl.GetDownlinkDecoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			}, c.WithClusterAuth())
			a.So(err, should.BeNil)
			conf.RequireAuth = false
		})
	})

	t.Run("GetDownlinkEncoder", func(t *testing.T) {
		t.Run("StoreError", func(t *testing.T) {
			a := assertions.New(t)
			st.err = fmt.Errorf("store error")
			models, err := cl.GetDownlinkEncoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a.So(models, should.BeNil)
			a.So(err.Error(), should.ContainSubstring, st.err.Error())
			a.So(st.lastVersionIDs, should.Resemble, ids)
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			st.downlinkEncoder = ttnpb.NewPopulatedMessagePayloadFormatter(test.Randy, true)
			st.err = nil

			c, err := cl.GetDownlinkEncoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a.So(err, should.BeNil)
			a.So(c, should.Resemble, st.downlinkEncoder)
		})

		t.Run("ClusterAuth", func(t *testing.T) {
			conf.RequireAuth = true
			codec, err := cl.GetDownlinkEncoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			})
			a := assertions.New(t)
			a.So(errors.IsUnauthenticated(err), should.BeTrue)
			a.So(codec, should.BeNil)

			_, err = cl.GetDownlinkEncoder(test.Context(), &ttnpb.GetPayloadFormatterRequest{
				VersionIDs: ids,
			}, c.WithClusterAuth())
			a.So(err, should.BeNil)
			conf.RequireAuth = false
		})
	})

	// conf is device repository config
	// st is device repository store
	// cl is device repository client

	// TODO: tests
}
