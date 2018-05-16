// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package containerstore

import "time"

type ContainerInfo struct {
	Account                 string
	Container               string
	CreatedAt               time.Time
	PutTimestamp            time.Time
	DeleteTimestamp         time.Time
	ReportedPutTimestamp    time.Time
	ReportedDeleteTimestamp time.Time
	ReportedObjectCount     uint64
	ReportedBytesUsed       uint64
	Hash                    [16]byte
	Id                      string
	Status                  string
	StatusChangedAt         time.Time
	StoragePolicyIndex      int64
	ContainerSyncPoint1     int64
	ContainerSyncPoint2     int64
	ReconcilerSyncPoint     int64
	Metadata                []MetadataItem
}

type CreateContainerRequest struct {
	Account            string
	Container          string
	Timestamp          time.Time
	StoragePolicyIndex int64
	Metadata           []MetadataItem
}

func defaultContainerInfo() *ContainerInfo {
	zeroTime := time.Unix(0, 0)

	return &ContainerInfo{
		ContainerSyncPoint1:     -1,
		ContainerSyncPoint2:     -1,
		ReconcilerSyncPoint:     -1,
		CreatedAt:               zeroTime,
		PutTimestamp:            zeroTime,
		DeleteTimestamp:         zeroTime,
		ReportedPutTimestamp:    zeroTime,
		ReportedDeleteTimestamp: zeroTime,
		StatusChangedAt:         zeroTime,
	}
}
