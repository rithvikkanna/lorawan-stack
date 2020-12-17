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
	"archive/zip"
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

// archiver archives and extracts data from zip archives.
type archiver struct{}

func (a *archiver) Archive(sourceDirectory, destinationFile string, fileFilter func(string) (string, bool)) error {
	f, err := os.Create(destinationFile)
	if err != nil {
		return err
	}
	z := zip.NewWriter(f)
	if err := filepath.Walk(sourceDirectory, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		pathInArchive, ok := fileFilter(path)
		if !ok {
			return nil
		}
		w, err := z.Create(pathInArchive)
		if err != nil {
			return err
		}
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return z.Close()
}

func (a *archiver) Unarchive(b []byte, destinationDirectory string) error {
	rd := bytes.NewReader(b)
	archive, err := zip.NewReader(rd, rd.Size())
	if err != nil {
		return err
	}

	for _, file := range archive.File {
		destination := path.Join(destinationDirectory, file.Name)
		if err := os.MkdirAll(path.Dir(destination), 0755); err != nil {
			return err
		}
		r, err := file.Open()
		if err != nil {
			return err
		}
		defer r.Close()
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		if err := ioutil.WriteFile(path.Join(destinationDirectory, file.Name), b, file.Mode()); err != nil {
			return err
		}
	}
	return nil
}
