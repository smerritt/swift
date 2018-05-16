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

package pathhasher

import (
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func decodeHex(h string) string {
	dh, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	return string(dh)
}

func TestHasher(t *testing.T) {
	assert := assert.New(t)

	h := New([]byte{}, []byte{})
	phash := h.HashAccountPath("AUTH_test")
	expected := decodeHex("50556319ff183c6ba65df78853cf2eca")
	assert.Equal(string(expected), phash)

	expected = decodeHex("3ce5a157c48a352782b5d43819f196ce")
	assert.Equal(string(expected), h.HashContainerPath("AUTH_test", "pictures_of_brunch"))

	expected = decodeHex("ecce361f27f19c14f1fc50d092470342")
	assert.Equal(string(expected), h.HashObjectPath("AUTH_test", "pictures_of_brunch", "avocado-toast.png"))
}

func TestWithPrefixAndSuffix(t *testing.T) {
	assert := assert.New(t)

	h := New([]byte("super"), []byte("secret"))

	expected := decodeHex("fa2bf4e78b8dc62b605f5f85d687228c")
	assert.Equal(string(expected), h.HashAccountPath("AUTH_test"))

	expected = decodeHex("a06a50aef86fbf84f3c74798c9db36da")
	assert.Equal(string(expected), h.HashContainerPath("AUTH_test", "pictures_of_brunch"))

	expected = decodeHex("a65dd61d96a70dd9546d416159db21b2")
	assert.Equal(string(expected), h.HashObjectPath("AUTH_test", "pictures_of_brunch", "avocado-toast.png"))
}

func TestFromConfFile(t *testing.T) {
	assert := assert.New(t)
	tempDir, err := ioutil.TempDir("", "pathhasher_test.go")
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}
	defer os.RemoveAll(tempDir)

	swiftConfPath := filepath.Join(tempDir, "swift.conf")
	f, err := os.OpenFile(swiftConfPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	_, err = io.WriteString(f, "[swift-hash]\nswift_hash_path_prefix=superduper\nswift_hash_path_suffix=topsecret\n")
	if err != nil {
		panic(err)
	}
	f.Close()

	h, err := FromSwiftConf(swiftConfPath)
	assert.Nil(err)

	// Make sure we loaded the prefix and suffix
	assert.Equal(h.prefix, []byte("superduper"))
	assert.Equal(h.suffix, []byte("topsecret"))

	// More importantly, make sure the prefix and suffix are actually applied
	expected := decodeHex("93867360d371456796031568d751cee6")
	assert.Equal(string(expected), h.HashObjectPath("AUTH_test", "catpictures", "yarnball.png"))
}

func TestFromConfFileMissingFile(t *testing.T) {
	assert := assert.New(t)

	tempDir, err := ioutil.TempDir("", "pathhasher_test.go")
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}
	defer os.RemoveAll(tempDir)

	swiftConfPath := filepath.Join(tempDir, "swift.conf")

	_, err = FromSwiftConf(swiftConfPath)
	assert.NotNil(err)
}

func TestFromConfFileMissingSection(t *testing.T) {
	assert := assert.New(t)

	tempDir, err := ioutil.TempDir("", "pathhasher_test.go")
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}
	defer os.RemoveAll(tempDir)

	swiftConfPath := filepath.Join(tempDir, "swift.conf")
	f, err := os.OpenFile(swiftConfPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	_, err = io.WriteString(f, "[other-stuff]\nkey1 = value1\nkey2 = value2  # comment\n")
	if err != nil {
		panic(err)
	}
	f.Close()

	h, err := FromSwiftConf(swiftConfPath)

	// No section -> empty values
	assert.Equal(h.prefix, []byte(""))
	assert.Equal(h.suffix, []byte(""))

	h, err = FromSwiftConf(swiftConfPath)
	assert.Nil(err)

	expected := decodeHex("6f252fe1594cbc7143aa55419129127a")
	assert.Equal(string(expected), h.HashObjectPath("AUTH_test", "catpictures", "yarnball.png"))
}

func TestFromConfFileMissingSuffix(t *testing.T) {
	// Not all swift.confs will have a suffix in them since Swift started with only a prefix.
	assert := assert.New(t)

	tempDir, err := ioutil.TempDir("", "pathhasher_test.go")
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}
	defer os.RemoveAll(tempDir)

	swiftConfPath := filepath.Join(tempDir, "swift.conf")
	f, err := os.OpenFile(swiftConfPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	_, err = io.WriteString(f, "[swift-hash]\nswift_hash_path_prefix = secretsquirrel\n")
	if err != nil {
		panic(err)
	}
	f.Close()

	h, err := FromSwiftConf(swiftConfPath)

	// No section -> empty values
	assert.Equal(h.prefix, []byte("secretsquirrel"))
	assert.Equal(h.suffix, []byte(""))

	h, err = FromSwiftConf(swiftConfPath)
	assert.Nil(err)

	expected := decodeHex("1f11bed9402f3da116f9c45e81ea6d2f")
	assert.Equal(string(expected), h.HashObjectPath("AUTH_test", "catpictures", "yarnball.png"))
}
