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
}
