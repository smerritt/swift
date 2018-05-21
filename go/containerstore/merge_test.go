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

func TestPartialMergeBogusValue(t *testing.T) {
	assert := assert.New(t)
	merger := mergeOperatorContainerAttributes{}

	hasher := pathhasher.New([]byte{}, []byte{})
	prefix := containerAttributeKeyPrefix(2872228, hasher.HashContainerPath("foo", "bar"))
	key := packMetadataKey(prefix, "X-Container-Meta-Utensil")

	t1 := time.Now()
	t2 := t1.Add(time.Second)
	value1 := packStringTimestampPair("fork", t1)
	value2 := packStringTimestampPair("spoon", t2)
	value2[0]++ // break it

	result, success := merger.PartialMerge(key, value1, value2)
	assert.False(success)
	assert.Equal([]byte{}, result)
}

func TestFullMerge(t *testing.T) {
	assert := assert.New(t)
	merger := mergeOperatorContainerAttributes{}

	t1 := time.Now()
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)
	t4 := t3.Add(time.Second)

	hasher := pathhasher.New([]byte{}, []byte{})
	prefix := containerAttributeKeyPrefix(2872228, hasher.HashContainerPath("macadamia", "gazebo"))
	key := packMetadataKey(prefix, "X-Container-Meta-Dishware")

	value1 := packStringTimestampPair("fork", t1)
	value2 := packStringTimestampPair("spoon", t2)
	value3 := packStringTimestampPair("knife", t3)
	value4 := packStringTimestampPair("cup", t4)

	result, success := merger.FullMerge(key, value1, [][]byte{value2, value3, value4})
	assert.True(success)
	assert.Equal(value4, result)

	// it works even if updates arrive at the DB in reverse chronological order
	result, success = merger.FullMerge(key, value4, [][]byte{value3, value2, value1})
	assert.True(success)
	assert.Equal(value4, result)

	// we may not have a base value; everything may have been inserted in the DB via Merge calls
	result, success = merger.FullMerge(key, []byte{}, [][]byte{value4, value3, value2, value1})
	assert.True(success)
	assert.Equal(value4, result)

	result, success = merger.FullMerge(key, []byte{}, [][]byte{value1, value2, value3, value4})
	assert.True(success)
	assert.Equal(value4, result)
}

func TestFullMergeBogusKey(t *testing.T) {
	assert := assert.New(t)
	merger := mergeOperatorContainerAttributes{}

	t1 := time.Now()
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)
	t4 := t3.Add(time.Second)

	hasher := pathhasher.New([]byte{}, []byte{})
	prefix := containerAttributeKeyPrefix(2872228, hasher.HashContainerPath("macadamia", "gazebo"))
	key := packMetadataKey(prefix, "X-Container-Meta-Dishware")
	key[len(key)-1]++

	value1 := packStringTimestampPair("fork", t1)
	value2 := packStringTimestampPair("spoon", t2)
	value3 := packStringTimestampPair("knife", t3)
	value4 := packStringTimestampPair("cup", t4)

	_, success := merger.FullMerge(key, value1, [][]byte{value2, value3, value4})
	assert.False(success)
}
