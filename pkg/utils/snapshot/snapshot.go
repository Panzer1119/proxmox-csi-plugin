/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package snapshot

import (
	"encoding/json"
	"fmt"
)

// NativeSnapshotID stores native ZFS snapshot metadata as a stable JSON handle.
type NativeSnapshotID struct {
	Region   string            `json:"region"`
	Zone     string            `json:"zone"`
	Storage  string            `json:"storage"`
	Dataset  string            `json:"dataset"`
	Snapshot string            `json:"snapshot"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// String serializes the snapshot ID as JSON.
func (n NativeSnapshotID) String() (string, error) {
	data, err := json.Marshal(n)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// ParseNativeSnapshotID parses a native snapshot JSON handle.
func ParseNativeSnapshotID(id string) (*NativeSnapshotID, error) {
	ref := &NativeSnapshotID{}
	if err := json.Unmarshal([]byte(id), ref); err != nil {
		return nil, err
	}
	if ref.Region == "" || ref.Storage == "" || ref.Dataset == "" || ref.Snapshot == "" {
		return nil, fmt.Errorf("invalid native snapshot id")
	}
	return ref, nil
}
