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
	_, err := cstore.GetContainer(123456, "LDAP_cowsays", "moo")
	assert.NotNil(err)
	_, ok := err.(*ContainerNotFoundError)
	assert.True(ok)

	// so we make it
	now := time.Now()
	meta := make([]MetadataItem, 0)
	meta = append(meta, MetadataItem{Name: "Breed", Value: "Holstein"})
	meta = append(meta, MetadataItem{Name: "Color", Value: "Black+White"})
	err = cstore.CreateContainer(CreateContainerRequest{
		Partition:          123456,
		Account:            "LDAP_cowsays",
		Container:          "moo",
		Timestamp:          now,
		StoragePolicyIndex: 5,
		Metadata:           meta})

	// Now it's there
	cinfo, err := cstore.GetContainer(123456, "LDAP_cowsays", "moo")
	assert.Nil(err)
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

func TestContainerDeletion(t *testing.T) {
	assert := assert.New(t)
	cstore, cleanup := makeTestDb()
	defer cleanup()

	accountName := "KEY_housewifery"
	containerName := "cyprine"

	now := time.Now()
	meta := make([]MetadataItem, 0)
	err := cstore.CreateContainer(CreateContainerRequest{
		Partition:          123,
		Account:            accountName,
		Container:          containerName,
		Timestamp:          now,
		StoragePolicyIndex: 0,
		Metadata:           meta})
	assert.Nil(err)

	// delete it
	now = now.Add(time.Second)
	err = cstore.DeleteContainer(DeleteContainerRequest{
		Partition: 123,
		Account:   accountName,
		Container: containerName,
		Timestamp: now,
	})
	assert.Nil(err)

	// and it's not there any more
	_, err = cstore.GetContainer(123, accountName, containerName)
	assert.NotNil(err)
	_, ok := err.(*ContainerNotFoundError)
	assert.True(ok)
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
	err := cstore.CreateContainer(CreateContainerRequest{
		Partition:          123456,
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

	err = cstore.UpdateContainer(UpdateContainerRequest{
		Partition: 123456,
		Account:   accountName,
		Container: containerName,
		Timestamp: now,
		Metadata:  updates,
	})
	assert.Nil(err)

	cinfo, err := cstore.GetContainer(123456, accountName, containerName)
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
	assert := assert.New(t)
	cstore, cleanup := makeTestDb()
	defer cleanup()

	accountName := "incommodement"
	containerName := "maladventure"

	now := time.Now()
	meta := make([]MetadataItem, 0)
	meta = append(meta, MetadataItem{Name: "SerialNumber", Value: "1"})
	err := cstore.CreateContainer(CreateContainerRequest{
		Partition:          12345,
		Account:            accountName,
		Container:          containerName,
		Timestamp:          now,
		StoragePolicyIndex: 8,
		Metadata:           meta})
	assert.Nil(err)

	for i := 0; i < 5; i++ {
		now = now.Add(time.Second)
		err = cstore.UpdateContainer(UpdateContainerRequest{
			Partition: 12345,
			Account:   accountName,
			Container: containerName,
			Timestamp: now,
			Metadata: []MetadataItem{
				MetadataItem{Name: "SerialNumber", Value: fmt.Sprintf("%d", i*100)}},
		})
		assert.Nil(err)
	}
	cinfo, err := cstore.GetContainer(12345, accountName, containerName)
	assert.Nil(err)
	assert.Equal(1, len(cinfo.Metadata))
	assert.Equal(cinfo.Metadata[0].Name, "SerialNumber")
	assert.Equal(cinfo.Metadata[0].Value, "400")
}

func TestContainerResurrection(t *testing.T) {
	assert := assert.New(t)

	cstore, cleanup := makeTestDb()
	defer cleanup()

	accountName := "valuate"
	containerName := "stypticity"

	// make a container
	now := time.Now()
	meta := make([]MetadataItem, 0)
	meta = append(meta, MetadataItem{Name: "OldMeta", Value: "dontcare"})
	err := cstore.CreateContainer(CreateContainerRequest{
		Partition:          12345,
		Account:            accountName,
		Container:          containerName,
		Timestamp:          now,
		StoragePolicyIndex: 8,
		Metadata:           meta})
	assert.Nil(err)

	// delete it
	now = now.Add(time.Second)
	err = cstore.DeleteContainer(DeleteContainerRequest{
		Partition: 12345,
		Account:   accountName,
		Container: containerName,
		Timestamp: now,
	})
	assert.Nil(err)

	// and resurrect it
	now = now.Add(time.Second)
	meta = make([]MetadataItem, 0)
	meta = append(meta, MetadataItem{Name: "NewMeta", Value: "goodstuff"})
	err = cstore.CreateContainer(CreateContainerRequest{
		Partition:          12345,
		Account:            accountName,
		Container:          containerName,
		Timestamp:          now,
		StoragePolicyIndex: 8,
		Metadata:           meta})
	assert.Nil(err)

	cinfo, err := cstore.GetContainer(12345, accountName, containerName)
	assert.Nil(err)
	assert.Equal(1, len(cinfo.Metadata))
	assert.Equal(cinfo.Metadata[0].Name, "NewMeta")
	assert.Equal(cinfo.Metadata[0].Value, "goodstuff")
}

func TestResurrectContainerInPast(t *testing.T) {
	assert := assert.New(t)
	cstore, cleanup := makeTestDb()
	defer cleanup()

	accountName := "Zygosaccharomyces"
	containerName := "paranormal"

	// make a container
	now := time.Now()
	meta := make([]MetadataItem, 0)
	err := cstore.CreateContainer(CreateContainerRequest{
		Partition:          12345,
		Account:            accountName,
		Container:          containerName,
		Timestamp:          now,
		StoragePolicyIndex: 8,
		Metadata:           meta})
	assert.Nil(err)

	// delete it
	now = now.Add(time.Second)
	err = cstore.DeleteContainer(DeleteContainerRequest{
		Partition: 12345,
		Account:   accountName,
		Container: containerName,
		Timestamp: now,
	})
	assert.Nil(err)

	// try to resurrect it, but with a timestamp earlier than the deletion
	now = now.Add(-time.Second * 30)
	err = cstore.CreateContainer(CreateContainerRequest{
		Partition:          12345,
		Account:            accountName,
		Container:          containerName,
		Timestamp:          now,
		StoragePolicyIndex: 8,
		Metadata:           meta})
	assert.NotNil(err)
}
