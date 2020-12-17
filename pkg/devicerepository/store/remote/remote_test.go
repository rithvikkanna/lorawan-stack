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

package remote_test

import (
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/smartystreets/assertions"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store/remote"
	"go.thethings.network/lorawan-stack/v3/pkg/errors"
	"go.thethings.network/lorawan-stack/v3/pkg/fetch"
	"go.thethings.network/lorawan-stack/v3/pkg/ttnpb"
	"go.thethings.network/lorawan-stack/v3/pkg/util/test/assertions/should"
)

var (
	data = map[string][]byte{
		"vendor/index.yaml": []byte(`
vendors:
- id: foo-vendor
  name: Foo Vendor
  vendorID: 42
- id: full-vendor
  name: Full Vendor
  vendorID: 44
  email: mail@example.com
  website: example.org
  pen: 42
  ouis: ["010203", "030405"]
  logo: logo.svg
- id: draft-vendor
  name: Draft
  description: Vendor that should not be returned
  draft: true`),
		"vendor/foo-vendor/index.yaml": []byte(`
endDevices:
- dev1
- dev2`),
		"vendor/foo-vendor/dev1.yaml": []byte(`
name: Device 1
description: My Description
hardwareVersions:
- version: 1.0
  numeric: 1
  partNumber: P4RTN0
firmwareVersions:
- version: 1.0
  hardwareVersions:
  - 1.0
  profiles:
    EU863-870: {id: profile1, lorawanCertified: true}
    US902-928: {codec: foo-codec, id: profile2, lorawanCertified: true}`),
		"vendor/foo-vendor/dev2.yaml": []byte(`
name: Device 2
description: My Description 2
hardwareVersions:
- version: 2.0
  numeric: 2
  partNumber: P4RTN02
firmwareVersions:
- version: 1.1
  hardwareVersions: [2.0]
  profiles:
   EU433: {codec: foo-codec, id: profile2, lorawanCertified: true}
sensors:
- temperature`),
		"vendor/foo-vendor/profile1.yaml": []byte(`
supportsClassB: false
supportsClassC: false
macVersion: 1.0.3
regionalParametersVersion: RP001-1.0.3-RevA
supportsJoin: true
maxEIRP: 27
supports32bitFCnt: true
`),
		"vendor/foo-vendor/profile2.yaml": []byte(`
supportsClassB: false
supportsClassC: false
macVersion: 1.0.2
regionalParametersVersion: RP001-1.0.2-RevB
supportsJoin: true
maxEIRP: 16
supports32bitFCnt: true
`),
		"vendor/foo-vendor/foo-codec.yaml": []byte(`
uplinkDecoder: {fileName: a.js}
downlinkDecoder: {fileName: b.js}
downlinkEncoder: {fileName: c.js}`),
		"vendor/foo-vendor/a.js": []byte("uplink decoder"),
		"vendor/foo-vendor/b.js": []byte("downlink decoder"),
		"vendor/foo-vendor/c.js": []byte("downlink encoder"),

		"vendor/full-vendor/index.yaml": []byte(`endDevices: [full-device]`),
		// full-vendor/full-device sets and tests all fields
		"vendor/full-vendor/full-device.yaml": []byte(`
name: Full Device
description: A description
hardwareVersions:
- version: 0.1
  numeric: 1
  partNumber: 0A0B
- version: 0.2
  numeric: 2
  partNumber: 0A0C
firmwareVersions:
  - version: 1.0
    hardwareVersions: [0.1, 0.2]
    profiles:
      EU863-870: {id: full-profile2}
      US902-928: {id: full-profile, codec: codec}
sensors: [temperature, gas]
dimensions:
  width: 1
  height: 2
  diameter: 3
  length: 4
weight: 5
battery:
  replaceable: true
  type: AAA
operatingConditions:
  temperature: {min: 1, max: 2}
  relativeHumidity: {min: 3, max: 4}
ipCode: IP67
keyProvisioning: [custom]
keySecurity: read protected
photos:
  main: a.jpg
  other: [b.jpg, c.jpg]
videos:
  main: a.mp4
  other: [b.mp4, "https://youtube.com/watch?v=c.mp4"]
productURL: https://product.vendor.io
datasheetURL: https://production.vendor.io/datasheet.pdf
compliances:
  safety:
  - {body: IEC, norm: EN, standard: 62368-1}
  - {body: IEC, norm: EN, standard: 60950-22}
  radioEquipment:
  - {body: ETSI, norm: EN, standard: 301 489-1, version: 2.2.0}
  - {body: ETSI, norm: EN, standard: 301 489-3, version: 2.1.0}
additionalRadios: [nfc, wifi]`),
	}
)

func TestRemoteStore(t *testing.T) {
	a := assertions.New(t)

	s := remote.NewRemoteStore(fetch.NewMemFetcher(data))

	t.Run("TestGetBrands", func(t *testing.T) {

		t.Run("Limit", func(t *testing.T) {
			list, err := s.GetBrands(store.GetBrandsRequest{
				Paths: []string{
					"brand_id",
					"name",
				},
				Limit: 1,
			})
			a.So(err, should.BeNil)
			a.So(list.Brands, should.Resemble, []*ttnpb.EndDeviceBrand{
				{
					BrandID: "foo-vendor",
					Name:    "Foo Vendor",
				},
			})
		})

		t.Run("SecondPage", func(t *testing.T) {
			list, err := s.GetBrands(store.GetBrandsRequest{
				Paths: []string{
					"brand_id",
					"name",
				},
				Limit: 1,
				Page:  2,
			})
			a.So(err, should.BeNil)
			a.So(list.Brands, should.Resemble, []*ttnpb.EndDeviceBrand{
				{
					BrandID: "full-vendor",
					Name:    "Full Vendor",
				},
			})
		})

		t.Run("Paths", func(t *testing.T) {
			list, err := s.GetBrands(store.GetBrandsRequest{
				Paths: ttnpb.EndDeviceBrandFieldPathsNested,
			})
			a.So(err, should.BeNil)
			a.So(list.Brands, should.Resemble, []*ttnpb.EndDeviceBrand{
				{
					BrandID:              "foo-vendor",
					Name:                 "Foo Vendor",
					LoRaAllianceVendorID: 42,
				},
				{
					BrandID:                       "full-vendor",
					Name:                          "Full Vendor",
					LoRaAllianceVendorID:          44,
					Email:                         "mail@example.com",
					Website:                       "example.org",
					PrivateEnterpriseNumber:       42,
					OrganizationUniqueIdentifiers: []string{"010203", "030405"},
					Logo:                          "logo.svg",
				},
			})
		})
	})

	t.Run("TestGetModels", func(t *testing.T) {
		t.Run("AllBrands", func(t *testing.T) {
			list, err := s.GetModels(store.GetModelsRequest{
				Paths: []string{
					"brand_id",
					"model_id",
					"name",
				},
			})
			a.So(err, should.BeNil)
			a.So(list.Models, should.Resemble, []*ttnpb.EndDeviceModel{
				{
					BrandID: "foo-vendor",
					ModelID: "dev1",
					Name:    "Device 1",
				},
				{
					BrandID: "foo-vendor",
					ModelID: "dev2",
					Name:    "Device 2",
				},
				{
					BrandID: "full-vendor",
					ModelID: "full-device",
					Name:    "Full Device",
				},
			})
		})

		t.Run("Limit", func(t *testing.T) {
			list, err := s.GetModels(store.GetModelsRequest{
				BrandID: "foo-vendor",
				Limit:   1,
				Paths: []string{
					"brand_id",
					"model_id",
					"name",
				},
			})
			a.So(err, should.BeNil)
			a.So(list.Models, should.Resemble, []*ttnpb.EndDeviceModel{
				{
					BrandID: "foo-vendor",
					ModelID: "dev1",
					Name:    "Device 1",
				},
			})
		})

		t.Run("Offset", func(t *testing.T) {
			list, err := s.GetModels(store.GetModelsRequest{
				BrandID: "foo-vendor",
				Limit:   1,
				Page:    2,
				Paths: []string{
					"brand_id",
					"model_id",
					"name",
				},
			})
			a.So(err, should.BeNil)
			a.So(list.Models, should.Resemble, []*ttnpb.EndDeviceModel{
				{
					BrandID: "foo-vendor",
					ModelID: "dev2",
					Name:    "Device 2",
				},
			})
		})

		t.Run("Paths", func(t *testing.T) {
			list, err := s.GetModels(store.GetModelsRequest{
				BrandID: "foo-vendor",
				Paths:   ttnpb.EndDeviceModelFieldPathsNested,
			})
			a.So(err, should.BeNil)
			a.So(list.Models, should.Resemble, []*ttnpb.EndDeviceModel{
				{
					BrandID:     "foo-vendor",
					ModelID:     "dev1",
					Name:        "Device 1",
					Description: "My Description",
					HardwareVersions: []*ttnpb.EndDeviceModel_HardwareVersion{
						{
							Version:    "1.0",
							Numeric:    1,
							PartNumber: "P4RTN0",
						},
					},
					FirmwareVersions: []*ttnpb.EndDeviceModel_FirmwareVersion{
						{
							Version:                   "1.0",
							SupportedHardwareVersions: []string{"1.0"},
							Profiles: map[string]*ttnpb.EndDeviceModel_FirmwareVersion_Profile{
								"EU_863_870": {
									ProfileID:        "profile1",
									LoRaWANCertified: true,
								},
								"US_902_928": {
									CodecID:          "foo-codec",
									ProfileID:        "profile2",
									LoRaWANCertified: true,
								},
							},
						},
					},
				},
				{
					BrandID:     "foo-vendor",
					ModelID:     "dev2",
					Name:        "Device 2",
					Description: "My Description 2",
					HardwareVersions: []*ttnpb.EndDeviceModel_HardwareVersion{
						{
							Version:    "2.0",
							Numeric:    2,
							PartNumber: "P4RTN02",
						},
					},
					FirmwareVersions: []*ttnpb.EndDeviceModel_FirmwareVersion{
						{
							Version:                   "1.1",
							SupportedHardwareVersions: []string{"2.0"},
							Profiles: map[string]*ttnpb.EndDeviceModel_FirmwareVersion_Profile{
								"EU_433": {
									CodecID:          "foo-codec",
									ProfileID:        "profile2",
									LoRaWANCertified: true,
								},
							},
						},
					},
					Sensors: []string{"temperature"},
				},
			})
		})

		t.Run("Full", func(t *testing.T) {
			a := assertions.New(t)
			list, err := s.GetModels(store.GetModelsRequest{
				BrandID: "full-vendor",
				Paths:   ttnpb.EndDeviceModelFieldPathsNested,
			})
			a.So(err, should.BeNil)
			a.So(list.Models[0], should.Resemble, &ttnpb.EndDeviceModel{
				BrandID:     "full-vendor",
				ModelID:     "full-device",
				Name:        "Full Device",
				Description: "A description",
				HardwareVersions: []*ttnpb.EndDeviceModel_HardwareVersion{
					{
						Version:    "0.1",
						Numeric:    1,
						PartNumber: "0A0B",
					},
					{
						Version:    "0.2",
						Numeric:    2,
						PartNumber: "0A0C",
					},
				},
				FirmwareVersions: []*ttnpb.EndDeviceModel_FirmwareVersion{
					{
						Version:                   "1.0",
						SupportedHardwareVersions: []string{"0.1", "0.2"},
						Profiles: map[string]*ttnpb.EndDeviceModel_FirmwareVersion_Profile{
							"EU_863_870": {
								CodecID:   "",
								ProfileID: "full-profile2",
							},
							"US_902_928": {
								CodecID:   "codec",
								ProfileID: "full-profile",
							},
						},
					},
				},
				Sensors: []string{"temperature", "gas"},
				Dimensions: &ttnpb.EndDeviceModel_Dimensions{
					Width:    &pbtypes.FloatValue{Value: 1},
					Height:   &pbtypes.FloatValue{Value: 2},
					Diameter: &pbtypes.FloatValue{Value: 3},
					Length:   &pbtypes.FloatValue{Value: 4},
				},
				Weight: &pbtypes.FloatValue{Value: 5},
				Battery: &ttnpb.EndDeviceModel_Battery{
					Replaceable: &pbtypes.BoolValue{Value: true},
					Type:        "AAA",
				},
				OperatingConditions: &ttnpb.EndDeviceModel_OperatingConditions{
					Temperature: &ttnpb.EndDeviceModel_OperatingConditions_Limits{
						Min: &pbtypes.FloatValue{Value: 1},
						Max: &pbtypes.FloatValue{Value: 2},
					},
					RelativeHumidity: &ttnpb.EndDeviceModel_OperatingConditions_Limits{
						Min: &pbtypes.FloatValue{Value: 3},
						Max: &pbtypes.FloatValue{Value: 4},
					},
				},
				IPCode:          "IP67",
				KeyProvisioning: []ttnpb.KeyProvisioning{ttnpb.KEY_PROVISIONING_CUSTOM},
				KeySecurity:     ttnpb.KEY_SECURITY_READ_PROTECTED,
				Photos: &ttnpb.EndDeviceModel_Photos{
					Main:  "a.jpg",
					Other: []string{"b.jpg", "c.jpg"},
				},
				Videos: &ttnpb.EndDeviceModel_Videos{
					Main:  "a.mp4",
					Other: []string{"b.mp4", "https://youtube.com/watch?v=c.mp4"},
				},
				ProductURL:   "https://product.vendor.io",
				DatasheetURL: "https://production.vendor.io/datasheet.pdf",
				Compliances: &ttnpb.EndDeviceModel_Compliances{
					Safety: []*ttnpb.EndDeviceModel_Compliances_Compliance{
						{
							Body:     "IEC",
							Norm:     "EN",
							Standard: "62368-1",
						},
						{
							Body:     "IEC",
							Norm:     "EN",
							Standard: "60950-22",
						},
					},
					RadioEquipment: []*ttnpb.EndDeviceModel_Compliances_Compliance{
						{
							Body:     "ETSI",
							Norm:     "EN",
							Standard: "301 489-1",
							Version:  "2.2.0",
						},
						{
							Body:     "ETSI",
							Norm:     "EN",
							Standard: "301 489-3",
							Version:  "2.1.0",
						},
					},
				},
				AdditionalRadios: []string{"nfc", "wifi"},
			})
		})
	})

	t.Run("TestGetCodecs", func(t *testing.T) {
		t.Run("Missing", func(t *testing.T) {
			a := assertions.New(t)

			for _, ids := range []ttnpb.EndDeviceVersionIdentifiers{
				{
					BrandID: "unknown-vendor",
				},
				{
					BrandID: "foo-vendor",
					ModelID: "unknown-model",
				},
				{
					BrandID:         "foo-vendor",
					ModelID:         "dev1",
					FirmwareVersion: "unknown-version",
				},
				{
					BrandID:         "foo-vendor",
					ModelID:         "dev1",
					FirmwareVersion: "1.0",
					BandID:          "unknown-band",
				},
			} {
				codec, err := s.GetDownlinkDecoder(&ids)
				a.So(errors.IsNotFound(err), should.BeTrue)
				a.So(codec, should.Equal, nil)
			}
		})
		for _, tc := range []struct {
			name  string
			f     func(*ttnpb.EndDeviceVersionIdentifiers) (*ttnpb.MessagePayloadFormatter, error)
			codec string
		}{
			{
				name:  "UplinkDecoder",
				f:     s.GetUplinkDecoder,
				codec: "uplink decoder",
			},
			{
				name:  "DownlinkDecoder",
				f:     s.GetDownlinkDecoder,
				codec: "downlink decoder",
			},
			{
				name:  "DownlinkEncoder",
				f:     s.GetDownlinkEncoder,
				codec: "downlink encoder",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				a := assertions.New(t)

				versionIDs := &ttnpb.EndDeviceVersionIdentifiers{
					BrandID:         "foo-vendor",
					ModelID:         "dev2",
					FirmwareVersion: "1.1",
					BandID:          "EU_433",
				}
				codec, err := tc.f(versionIDs)
				a.So(err, should.BeNil)
				a.So(codec, should.Resemble, &ttnpb.MessagePayloadFormatter{
					Formatter:          ttnpb.PayloadFormatter_FORMATTER_JAVASCRIPT,
					FormatterParameter: tc.codec,
				})
			})
		}
	})

	t.Run("GetTemplate", func(t *testing.T) {
		t.Run("Missing", func(t *testing.T) {
			a := assertions.New(t)

			for _, ids := range []ttnpb.EndDeviceVersionIdentifiers{
				{
					BrandID: "unknown-vendor",
				},
				{
					BrandID: "foo-vendor",
					ModelID: "unknown-model",
				},
				{
					BrandID:         "foo-vendor",
					ModelID:         "dev1",
					FirmwareVersion: "unknown-version",
				},
				{
					BrandID:         "foo-vendor",
					ModelID:         "dev1",
					FirmwareVersion: "1.0",
					BandID:          "unknown-band",
				},
			} {
				tmpl, err := s.GetTemplate(&ids)
				a.So(errors.IsNotFound(err), should.BeTrue)
				a.So(tmpl, should.BeNil)
			}
		})

		t.Run("Success", func(t *testing.T) {
			a := assertions.New(t)
			tmpl, err := s.GetTemplate(&ttnpb.EndDeviceVersionIdentifiers{
				BrandID:         "foo-vendor",
				ModelID:         "dev2",
				FirmwareVersion: "1.1",
				HardwareVersion: "2.0",
				BandID:          "EU_433",
			})
			a.So(err, should.BeNil)
			a.So(tmpl, should.NotBeNil)
		})
	})
}
