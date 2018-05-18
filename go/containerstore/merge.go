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

func mergeContainerMetadataValues(left, right []byte) ([]byte, error) {
	_, leftTime, err := unpackStringTimestampPair(left)
	if err != nil {
		return []byte{}, err
	}
	_, rightTime, err := unpackStringTimestampPair(right)
	if err != nil {
		return []byte{}, err
	}

	// Newest wins
	if leftTime.After(rightTime) {
		return left, nil
	} else {
		return right, nil
	}
}

func partialMergeContainerAttributes(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	// both leftOperand and rightOperand are the values passed to Merge() calls
	_, attrType, _, err := unpackAttributeKey(key)
	if err != nil {
		return []byte{}, false
	}

	switch attrType {
	case metadataType:
		newer, err := mergeContainerMetadataValues(leftOperand, rightOperand)
		if err != nil {
			return newer, false
		}
		return newer, true
	}

	// If it's a thing we don't know how to merge, then return false and hope that the full merge knows what to do with
	// it.
	return []byte{}, false
}

func fullMergeContainerAttributes(key []byte, existingValue []byte, mergeOperands [][]byte) ([]byte, bool) {
	_, attrType, _, err := unpackAttributeKey(key)
	if err != nil {
		// TODO: log an error
		return []byte{}, false
	}

	if attrType == metadataType {
		var result []byte
		var err error
		if len(existingValue) == 0 {
			// No existing value (from a Put), just some Merge operands.
			result = mergeOperands[0] // assume that we have a merge operand, otherwise WTF is rocks doing?
		} else {
			result, err = mergeContainerMetadataValues(existingValue, mergeOperands[0])
			if err != nil {
				// TODO: log an error here; if this happens, then we've got bogus values in the DB
				return []byte{}, false
			}
		}
		for i := 1; i < len(mergeOperands); i++ {
			result, err = mergeContainerMetadataValues(result, mergeOperands[i])
			if err != nil {
				// TODO: log an error here; if this happens, then we've got bogus values in the DB
				return []byte{}, false
			}
		}
		return result, true
	}

	// TODO: complain loudly here; this means someone's calling Merge but hasn't updated these functions to make it work
	return []byte{}, false
}

type mergeOperatorContainerAttributes struct{}

func (m *mergeOperatorContainerAttributes) Name() string {
	// Don't change this or RocksDB will complain. It has no meaning, but the string is stored in the DB at creation,
	// and if it doesn't match on subsequent OpenDb[ColumnFamilies] calls, then RocksDB won't open the database.
	return "attrmerge"
}

func (m *mergeOperatorContainerAttributes) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return fullMergeContainerAttributes(key, existingValue, operands)
}

func (m *mergeOperatorContainerAttributes) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return partialMergeContainerAttributes(key, leftOperand, rightOperand)
}
