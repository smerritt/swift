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
	"crypto/md5"

	"github.com/go-ini/ini"
	"github.com/pkg/errors"
)

type Hasher struct {
	prefix []byte
	suffix []byte
}

func New(prefix []byte, suffix []byte) Hasher {
	sh := Hasher{prefix: prefix, suffix: suffix}
	return sh
}

// Read prefix and suffix from swift.conf and return a new Hasher with those values.
//
// Returns an error if the config file could not be read. If the [swift-hash] section or the prefix/suffix are absent,
// that is not an error and empty strings will be used instead. This matches the behavior of Swift's Python code.

func FromSwiftConf(swiftConfPath string) (*Hasher, error) {
	prefix := ""
	suffix := ""

	swiftConf, err := ini.Load(swiftConfPath)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to load config file %s", swiftConfPath)
	}

	section, err := swiftConf.GetSection("swift-hash")
	if err == nil {
		// Key(x).String() results in empty-string if the key is missing.
		prefix = section.Key("swift_hash_path_prefix").String()
		suffix = section.Key("swift_hash_path_suffix").String()
	}

	sh := New([]byte(prefix), []byte(suffix))
	return &sh, nil
}

func (h *Hasher) HashAccountPath(acc string) string {
	return h.hashParts([]string{acc})
}

func (h *Hasher) HashContainerPath(acc string, con string) string {
	return h.hashParts([]string{acc, con})
}

func (h *Hasher) HashObjectPath(acc string, con string, obj string) string {
	return h.hashParts([]string{acc, con, obj})
}

func (h *Hasher) hashParts(parts []string) string {
	result := make([]byte, 0, 16)

	md5er := md5.New()
	// these writes only manipulate in-memory data, so they won't fail
	md5er.Write(h.prefix)
	for _, part := range parts {
		md5er.Write([]byte("/"))
		md5er.Write([]byte(part))
	}
	md5er.Write(h.suffix)
	return string(md5er.Sum(result))
}
