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

package commands

import (
	"github.com/spf13/cobra"
	"go.thethings.network/lorawan-stack/v3/pkg/devicerepository/store/index"
	"go.thethings.network/lorawan-stack/v3/pkg/fetch"
)

var (
	drCommand = &cobra.Command{
		Use:   "dr",
		Short: "Device Repository commands",
	}
	drCreateIndexCommand = &cobra.Command{
		Use:   "create-package",
		Short: "Create a new package for the Device Repository",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger.Info("Creating new index...")

			output, _ := cmd.Flags().GetString("output")
			if output == "" {
				return errMissingFlag.WithAttributes("flag", "output")
			}
			source, _ := cmd.Flags().GetString("source")
			if source == "" {
				return errMissingFlag.WithAttributes("flag", "source")
			}
			overwrite, _ := cmd.Flags().GetBool("overwrite")

			if err := index.CreatePackage(ctx, fetch.FromFilesystem(source), source, output, overwrite); err != nil {
				return err
			}
			logger.WithField("path", output).Info("Successfully created index")
			return nil
		},
	}
)

func init() {
	Root.AddCommand(drCommand)

	drCreateIndexCommand.Flags().String("output", "", "Place to create the new index")
	drCreateIndexCommand.Flags().String("source", "", "Path to root directory of lorawan-devices repository")
	drCreateIndexCommand.Flags().Bool("overwrite", false, "Overwrite previous index files")
	drCommand.AddCommand(drCreateIndexCommand)
}
