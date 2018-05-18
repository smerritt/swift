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

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/openstack/swift/go/pathhasher"
	"github.com/stretchr/testify/assert"
)

func makeTestDb() (*ContainerStore, func()) {
	tempDir, err := ioutil.TempDir("", "dbtest.go")
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}
	cleanupTempDir := func() {
		_ = os.RemoveAll(tempDir)
	}

	deviceName := "sda"
	err = os.Mkdir(filepath.Join(tempDir, deviceName), 0700)
	if err != nil {
		panic(fmt.Sprintf("failed making dir sda in testSetup: %v", err))
	}

	ph := pathhasher.New([]byte{}, []byte{})
	cstore, err := Open(tempDir, deviceName, ph)
	if err != nil {
		panic(fmt.Sprintf("failed making test RDB in testSetup: %v", err))
	}

	return cstore, cleanupTempDir
}

func TestContainerExistence(t *testing.T) {
	assert := assert.New(t)
	cstore, cleanup := makeTestDb()
	defer cleanup()

	// It's not there
	_, containerExists, err := cstore.GetContainer(123456, "LDAP_cowsays", "moo")
	assert.False(containerExists)
	assert.Nil(err)

	// so we make it
	now := time.Now()
	meta := make([]MetadataItem, 0)
	meta = append(meta, MetadataItem{Name: "Breed", Value: "Holstein"})
	meta = append(meta, MetadataItem{Name: "Color", Value: "Black+White"})
	err = cstore.CreateContainer(123456, CreateContainerRequest{
		Account:            "LDAP_cowsays",
		Container:          "moo",
		Timestamp:          now,
		StoragePolicyIndex: 5,
		Metadata:           meta})

	// Now it's there
	cinfo, containerExists, err := cstore.GetContainer(123456, "LDAP_cowsays", "moo")
	assert.Nil(err)
	assert.True(containerExists)
	assert.Equal("LDAP_cowsays", cinfo.Account)
	assert.Equal("moo", cinfo.Container)
	assert.True(now.Equal(cinfo.CreatedAt))
	assert.True(now.Equal(cinfo.PutTimestamp))
	assert.True(cinfo.DeleteTimestamp.Equal(time.Unix(0, 0)))
	assert.True(cinfo.ReportedPutTimestamp.Equal(time.Unix(0, 0)))
	assert.True(cinfo.ReportedDeleteTimestamp.Equal(time.Unix(0, 0)))
	assert.Equal(uint64(0), cinfo.ReportedObjectCount)
	assert.Equal(uint64(0), cinfo.ReportedBytesUsed)
	assert.Equal([16]byte{}, cinfo.Hash)
	assert.Regexp(regexp.MustCompile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$"), cinfo.Id)
	assert.True(now.Equal(cinfo.StatusChangedAt))
	assert.Equal("", cinfo.Status)
	assert.Equal(int64(5), cinfo.StoragePolicyIndex)
	assert.Equal(int64(-1), cinfo.ContainerSyncPoint1)
	assert.Equal(int64(-1), cinfo.ContainerSyncPoint2)
	assert.Equal(int64(-1), cinfo.ReconcilerSyncPoint)

	assert.Equal(2, len(cinfo.Metadata))
	assert.Equal("Breed", cinfo.Metadata[0].Name)
	assert.Equal("Holstein", cinfo.Metadata[0].Value)
	assert.True(now.Equal(cinfo.Metadata[0].Timestamp))
	assert.Equal("Color", cinfo.Metadata[1].Name)
	assert.Equal("Black+White", cinfo.Metadata[1].Value)
	assert.True(now.Equal(cinfo.Metadata[1].Timestamp))
}

func TestContainerMetadataUpdate(t *testing.T) {
	assert := assert.New(t)
	cstore, cleanup := makeTestDb()
	defer cleanup()

	accountName := "AUTH_Monilia"
	containerName := "phalanges"

	now := time.Now()
	meta := make([]MetadataItem, 0)
	meta = append(meta, MetadataItem{Name: "Calories", Value: "120"})
	meta = append(meta, MetadataItem{Name: "Flavor", Value: "raspberry"})
	meta = append(meta, MetadataItem{Name: "Cost", Value: "$1"})
	err := cstore.CreateContainer(123456, CreateContainerRequest{
		Account:            accountName,
		Container:          containerName,
		Timestamp:          now,
		StoragePolicyIndex: 12,
		Metadata:           meta})
	assert.Nil(err)

	// Update one metadatum, delete another, add a third
	now = now.Add(time.Second)
	updates := make([]MetadataItem, 0)
	updates = append(updates, MetadataItem{Name: "Calories", Value: "130"})
	updates = append(updates, MetadataItem{Name: "Category", Value: "Cookie"})
	updates = append(updates, MetadataItem{Name: "Flavor", Value: ""}) // empty value means deletion

	err = cstore.UpdateContainer(123456, UpdateContainerRequest{
		Account:   accountName,
		Container: containerName,
		Timestamp: now,
		Metadata:  updates,
	})
	assert.Nil(err)

	cinfo, containerExists, err := cstore.GetContainer(123456, accountName, containerName)
	assert.True(containerExists)
	assert.Nil(err)
	assert.Equal(3, len(cinfo.Metadata))
	assert.Equal(cinfo.Metadata[0].Name, "Calories")
	assert.Equal(cinfo.Metadata[0].Value, "130")
	assert.Equal(cinfo.Metadata[1].Name, "Category")
	assert.Equal(cinfo.Metadata[1].Value, "Cookie")
	assert.Equal(cinfo.Metadata[2].Name, "Cost")
	assert.Equal(cinfo.Metadata[2].Value, "$1")
}

func TestContainerMetadataMultipleUpdates(t *testing.T) {
	// This tests the partial merge operator
}
