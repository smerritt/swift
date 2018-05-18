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
	"time"

	"github.com/openstack/swift/go/pathhasher"
	"github.com/stretchr/testify/assert"
)

func TestPartialMergeContainerMeta(t *testing.T) {
	assert := assert.New(t)
	merger := mergeOperatorContainerAttributes{}

	t1 := time.Now()
	t2 := t1.Add(time.Second)

	hasher := pathhasher.New([]byte{}, []byte{})
	prefix := containerAttributeKeyPrefix(2872228, hasher.HashContainerPath("foo", "bar"))
	key := packMetadataKey(prefix, "X-Container-Meta-Utensil")
	value1 := packStringTimestampPair("fork", t1)
	value2 := packStringTimestampPair("spoon", t2)

	result, success := merger.PartialMerge(key, value1, value2)
	assert.True(success)
	assert.Equal(value2, result)

	// It's the older value that wins, and we determine "older" by the timestamp embedded in the value
	result, success = merger.PartialMerge(key, value2, value1)
	assert.True(success)
	assert.Equal(value2, result)
}

func TestPartialMergeBogusKey(t *testing.T) {
	assert := assert.New(t)
	merger := mergeOperatorContainerAttributes{}

	key := []byte{33, 44, 55}

	t1 := time.Now()
	t2 := t1.Add(time.Second)
	value1 := packStringTimestampPair("fork", t1)
	value2 := packStringTimestampPair("spoon", t2)

	result, success := merger.PartialMerge(key, value1, value2)
	assert.False(success)
	assert.Equal([]byte{}, result)
}
