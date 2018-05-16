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
	"testing"

	"github.com/openstack/swift/go/pathhasher"
	"github.com/stretchr/testify/assert"
)

func TestPackAttributeKey(t *testing.T) {
	assert := assert.New(t)

	partition := 9504099
	containerHash := pathhasher.New([]byte{}, []byte{}).HashContainerPath("fizz", "buzz")
	prefix := containerAttributeKeyPrefix(partition, containerHash)

	packedValue := packAttributeKey(prefix, hashType) // nothing special about hashType

	gotPrefix, gotAttributeType, err := unpackAttributeKey(packedValue)
	assert.Nil(err)
	assert.Equal(partition, gotPartition)
	assert.Equal(containerHash, gotContainerHash)
	assert.Equal(hashType, gotAttributeType)
}
