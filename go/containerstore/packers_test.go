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

// Tests for the various packX/unpackX functions

// [un]packAttributeKey
func TestPackAttributeKeyRoundTrip(t *testing.T) {
	assert := assert.New(t)

	partition := uint32(9504099)
	hasher := pathhasher.New([]byte{}, []byte{})
	containerHash := hasher.HashContainerPath("fizz", "buzz")
	prefix := containerAttributeKeyPrefix(partition, containerHash)

	packedValue := packAttributeKey(prefix, hashType) // nothing special about hashType

	gotPrefix, gotAttributeType, gotRemainder, err := unpackAttributeKey(packedValue)
	assert.Nil(err)
	assert.Equal(prefix, gotPrefix)
	assert.Equal([]byte{}, gotRemainder)
	assert.Equal(hashType, gotAttributeType)
}

func TestUnpackAttributeKeyShortInput(t *testing.T) {
	assert := assert.New(t)

	_, _, _, err := unpackAttributeKey([]byte{1, 2, 3, 0})
	assert.NotNil(err)
}

func TestUnpackAttributeKeyBadVersion(t *testing.T) {
	assert := assert.New(t)

	partition := uint32(2035124)
	hasher := pathhasher.New([]byte{}, []byte{})
	containerHash := hasher.HashContainerPath("tweed", "unfathomableness")
	prefix := containerAttributeKeyPrefix(partition, containerHash)

	packedValue := packAttributeKey(prefix, hashType) // nothing special about hashType
	packedValue[len(packedValue)-1]++

	_, _, _, err := unpackAttributeKey([]byte{1, 2, 3, 0})
	assert.NotNil(err)
}

// [un]packUint64Value
func TestUnpackUint64RoundTrip(t *testing.T) {
	assert := assert.New(t)

	value := uint64(4391666)
	gotValue, err := unpackUint64Value(packUint64Value(value))
	assert.Nil(err)
	assert.Equal(value, gotValue)
}

func TestUnpackUint64ShortInput(t *testing.T) {
	assert := assert.New(t)

	_, err := unpackUint64Value([]byte{})
	assert.NotNil(err)
}

func TestUnpackUint64WrongType(t *testing.T) {
	assert := assert.New(t)

	packed := packUint64Value(uint64(148190))
	packed[0]++ // change the type indicator (it's first in the value)
	_, err := unpackUint64Value([]byte{})
	assert.NotNil(err)
}

// [un]packInt64Value
func TestUnpackInt64RoundTrip(t *testing.T) {
	assert := assert.New(t)

	value := int64(4391666)
	gotValue, err := unpackInt64Value(packInt64Value(value))
	assert.Nil(err)
	assert.Equal(value, gotValue)
}

func TestUnpackInt64ShortInput(t *testing.T) {
	assert := assert.New(t)

	_, err := unpackInt64Value([]byte{})
	assert.NotNil(err)
}

func TestUnpackInt64WrongType(t *testing.T) {
	assert := assert.New(t)

	packed := packInt64Value(int64(148190))
	packed[0]++ // change the type indicator (it's first in the value)
	_, err := unpackInt64Value([]byte{})
	assert.NotNil(err)
}

// [un]packTimeValue
func TestUnpackTimeValueRoundTrip(t *testing.T) {
	assert := assert.New(t)

	theTime := time.Now()

	gotTime, err := unpackTimeValue(packTimeValue(theTime))
	assert.Nil(err)
	assert.True(gotTime.Equal(theTime))
}

func TestUnpackTimeValueShortInput(t *testing.T) {
	assert := assert.New(t)

	_, err := unpackTimeValue([]byte{})
	assert.NotNil(err)
}

func TestUnpackTimeValueWrongType(t *testing.T) {
	assert := assert.New(t)

	packed := packTimeValue(time.Now())
	packed[0]++ // change the type indicator
	_, err := unpackTimeValue([]byte{})
	assert.NotNil(err)
}

// [un]packStringValue
func TestUnpackStringValueRoundTrip(t *testing.T) {
	assert := assert.New(t)

	value := "skyman-Corylus"
	gotValue, err := unpackStringValue(packStringValue(value))
	assert.Nil(err)
	assert.Equal(value, gotValue)
}

func TestUnpackStringValueEmptyString(t *testing.T) {
	assert := assert.New(t)
	gotValue, err := unpackStringValue(packStringValue(""))
	assert.Nil(err)
	assert.Equal("", gotValue)
}

func TestUnpackStringValueShortInput(t *testing.T) {
	assert := assert.New(t)

	_, err := unpackStringValue([]byte{})
	assert.NotNil(err)
}

func TestUnpackStringValueWrongType(t *testing.T) {
	assert := assert.New(t)

	packed := packStringValue("ophiophilism-dendrophil")
	packed[0]++
	_, err := unpackStringValue(packed)
	assert.NotNil(err)
}

// [un]packMd5HashValue
func TestUnpackMd5HashValueRoundTrip(t *testing.T) {
	assert := assert.New(t)

	value := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	gotValue, err := unpackMd5HashValue(packMd5HashValue(value))
	assert.Nil(err)
	assert.Equal(value, gotValue)
}

func TestUnpackMd5HashValueShortInput(t *testing.T) {
	assert := assert.New(t)

	_, err := unpackMd5HashValue([]byte{})
	assert.NotNil(err)
}

func TestUnpackMd5HashValueWrongType(t *testing.T) {
	assert := assert.New(t)

	value := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	packed := packMd5HashValue(value)
	packed[0]++
	_, err := unpackMd5HashValue(packed)
	assert.NotNil(err)
}

func TestUnpackStringTimestampPair(t *testing.T) {
	assert := assert.New(t)
	strValue := "grysbok-trimembral"
	timeValue := time.Now()

	gotStr, gotTime, err := unpackStringTimestampPair(packStringTimestampPair(strValue, timeValue))
	assert.Nil(err)
	assert.Equal(strValue, gotStr)
	assert.True(timeValue.Equal(gotTime))
}

func TestUnpackStringTimestampPairEmptyString(t *testing.T) {
	assert := assert.New(t)
	strValue := ""
	timeValue := time.Now()

	gotStr, gotTime, err := unpackStringTimestampPair(packStringTimestampPair(strValue, timeValue))
	assert.Nil(err)
	assert.Equal(strValue, gotStr)
	assert.True(timeValue.Equal(gotTime))
}

func TestUnpackStringTimestampPairShortInput(t *testing.T) {
	assert := assert.New(t)

	_, _, err := unpackStringTimestampPair([]byte{})
	assert.NotNil(err)
}

func TestUnpackStringTimestampPairWrongType(t *testing.T) {
	assert := assert.New(t)
	packed := packStringTimestampPair("alemonger-Anseis", time.Now())
	packed[0]++
	_, _, err := unpackStringTimestampPair(packed)
	assert.NotNil(err)
}
