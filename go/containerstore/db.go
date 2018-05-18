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

// Data formats:
//
// We've got three different formats for data stored in RocksDB.
//
// Container attributes, including user metadata and system metadata, look like this:
//
//   Key: [
//         0x00              1 byte: metadataKeyPrefix (so we can store other types of stuff here later)
//
//         <partition>       4 bytes: uint32, network byte order
//
//         <container hash>  16 bytes: hash of container name
//
//         <datum type>      2 bytes, of type containerAttributeName
//
//         <datum>           variable length string; can be user metadata (like 'X-Container-Meta-Flavor'),
//                           system metadata, or empty (for fields like PutTimestamp)
//
//         0x00              1 byte: key version (always 0 until we need a new key format)
//        ]
//
//   Value: [
//           <type>  1 byte
//
//           value   variable length
//          ]
//

package containerstore

// TODO:
//
// * we've got constants hashType and md5HashType; this is confusing
//
// make a real type for MD5 hash values instead of just [16]byte

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/openstack/swift/go/pathhasher"
	"github.com/pkg/errors"
	"github.com/tecbot/gorocksdb"
)

// Don't change these values; they're part of the on-disk format.
const (
	metadataKeyPrefix = iota
	objByNameKeyPrefix
	objBySeqKeyPrefix
)

// Don't change these values; they're part of the on-disk format.
const (
	stringType = iota
	timeType
	uint64Type
	int64Type
	md5HashType
	stringTimestampPairType
)

// Don't change these values; they're part of the on-disk format.
type containerAttributeName uint16

const (
	accountType containerAttributeName = iota
	containerType
	createdAtType
	putTimestampType
	deleteTimestampType
	reportedPutTimestampType
	reportedDeleteTimestampType
	reportedObjectCountType
	reportedBytesUsedType
	hashType
	idType
	statusType
	statusChangedAtType
	storagePolicyIndexType
	containerSyncPoint1Type
	containerSyncPoint2Type
	reconcilerSyncPointType
	metadataType // also includes sysmeta
)

const (
	containerAttributePrefixSize  = 1 + 4 + 16 // prefix + partition + container hash
	containerAttributeNameSize    = 2          // size of uint16
	containerAttributeVersionSize = 1
)

type CreateContainerRequest struct {
	Account            string
	Container          string
	Timestamp          time.Time
	StoragePolicyIndex int64
	Metadata           []MetadataItem
}

type UpdateContainerRequest struct {
	Account   string
	Container string
	Timestamp time.Time
	Metadata  []MetadataItem
}

type ContainerStore struct {
	Device      string
	hasher      pathhasher.Hasher
	lockManager lockManager
	db          *gorocksdb.DB
	metaCF      *gorocksdb.ColumnFamilyHandle
	objCF       *gorocksdb.ColumnFamilyHandle
}

func Open(devicesPath string, device string, ph pathhasher.Hasher) (*ContainerStore, error) {
	fullPath := path.Join(devicesPath, device)

	// We use two column families here: one for object records, and one for other container info (e.g. storage policy,
	// user/sys-meta, object counts, chexor). This is because we have two vastly different access patterns.
	//
	// For container info, reads always fetch ranges of keys; we'll virtually never make individual Get calls. Also,
	// container info will have a lot of merges, since every object row Put or Delete will result in updates to the
	// object chexor and counts, so we want a separate column family for them so that those Merge records are
	// aggressively compacted together.
	//
	// For object rows, we'll have a lot of individual key lookups.
	metaOpts := gorocksdb.NewDefaultOptions()
	objOpts := gorocksdb.NewDefaultOptions()
	dbOpts := gorocksdb.NewDefaultOptions()

	// TODO: merge operator for metadata, chexor, object count (metaOpts)
	metaOpts.SetMergeOperator(&mergeOperatorContainerAttributes{})

	// TODO: compaction filter to strip out old tombstones (objOpts)

	// TODO: everything (everywhere)

	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCreateIfMissingColumnFamilies(true)
	// It'd look nicer to call them "meta" and "obj", but RocksDB throws a fit if you don't have a column family named
	// "default".
	cfNames := []string{"default", "obj"}
	cfOpts := []*gorocksdb.Options{metaOpts, objOpts}

	db, columnFamilyHandles, err := gorocksdb.OpenDbColumnFamilies(dbOpts, fullPath, cfNames, cfOpts)
	if err != nil {
		return nil, err
	}

	store := ContainerStore{Device: device, db: db, hasher: ph,
		metaCF: columnFamilyHandles[0], objCF: columnFamilyHandles[1],
		lockManager: newLockManager(),
	}
	return &store, nil
}

// Returns the prefix of all keys for this container
func containerAttributeKeyPrefix(partition uint32, containerHash string) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, containerAttributePrefixSize))

	// The returned errors here are always nil
	buf.WriteByte(metadataKeyPrefix)
	binary.Write(buf, binary.BigEndian, partition)
	buf.Write([]byte(containerHash))

	return buf.Bytes()
}

// Returns a key just beyond the range of the container's keys. This is used as an iterator's upper bound so that
// getting container info for container A stops before reading anything from container B.
func containerMetaKeyUpperBound(partition uint32, containerHash string) []byte {
	out := containerAttributeKeyPrefix(partition, containerHash)

	// Since we want one past the end, we need to increment the prefix to refer to the next container.
	//
	// This won't work for container-hash 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF in partition 2^32 - 1, but it'll work for
	// everyone else, and nobody runs with a partition power of 32 anyway.
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 255 {
			out[i] += 1
			break
		} else {
			out[i] = 0
		}
	}
	return out
}

func packAttributeKey(prefix []byte, name containerAttributeName) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, len(prefix)+3))
	buf.Write(prefix)
	binary.Write(buf, binary.BigEndian, uint16(name))
	buf.WriteByte(0) // key-format version
	return buf.Bytes()
}

func unpackAttributeKey(value []byte) (prefix []byte, name containerAttributeName, remainder []byte, err error) {
	minLength := containerAttributePrefixSize + containerAttributeNameSize + containerAttributeVersionSize
	if len(value) < minLength {
		err = fmt.Errorf("Couldn't unpack attribute key: wanted value of length >= %d, got %d",
			minLength, len(value))
		return
	} else if value[len(value)-1] != 0 {
		err = fmt.Errorf("Couldn't unpack attribute key: wanted version 0, got %d", value[len(value)-1])
	}

	prefix = value[:containerAttributePrefixSize]
	name = containerAttributeName(binary.BigEndian.Uint16(value[containerAttributePrefixSize:]))
	remainder = value[containerAttributePrefixSize+containerAttributeNameSize : len(value)-1]
	return
}

func packMetadataKey(prefix []byte, key string) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, len(prefix)+len(key)+1))
	buf.Write(prefix)
	binary.Write(buf, binary.BigEndian, uint16(metadataType))
	buf.WriteString(key)
	buf.WriteByte(0) // key-format version
	return buf.Bytes()
}

func packStringValue(value string) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, len(value)+1))
	buf.WriteByte(stringType)
	buf.WriteString(value)
	return buf.Bytes()
}

func unpackStringValue(value []byte) (string, error) {
	if len(value) < 1 {
		return "", fmt.Errorf("Couldn't unpack string value: input was empty")
	} else if value[0] != stringType {
		return "", fmt.Errorf("Couldn't unpack string value: wanted type-byte %d, got %d", stringType, value[0])
	}
	return string(value[1:]), nil
}

func packTimeValue(value time.Time) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 9))
	buf.WriteByte(timeType)
	binary.Write(buf, binary.BigEndian, value.UnixNano())
	return buf.Bytes()
}

func unpackTimeValue(value []byte) (time.Time, error) {
	if len(value) != 9 {
		return time.Time{}, fmt.Errorf("Couldn't unpack time value: wanted array of length 9, got %d", len(value))
	} else if value[0] != timeType {
		return time.Time{}, fmt.Errorf("Couldn't unpack time value: wanted type-byte %d, got %d", timeType, value[0])
	}

	nano := int64(binary.BigEndian.Uint64(value[1:]))
	return time.Unix(nano/1000000000, nano%1000000000), nil
}

func packUint64Value(value uint64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	buf.WriteByte(uint64Type)
	binary.Write(buf, binary.BigEndian, value)
	return buf.Bytes()
}

func unpackUint64Value(value []byte) (uint64, error) {
	if len(value) != 9 {
		return 0, fmt.Errorf("Couldn't unpack uint64 value: wanted array of length 9, got %d", len(value))
	} else if value[0] != uint64Type {
		return 0, fmt.Errorf("Couldn't unpack uint64 value: wanted type-byte %d, got %d", uint64Type, value[0])
	}
	return binary.BigEndian.Uint64(value[1:]), nil
}

func packInt64Value(value int64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	buf.WriteByte(int64Type)
	binary.Write(buf, binary.BigEndian, value)
	return buf.Bytes()
}

func unpackInt64Value(value []byte) (int64, error) {
	var out int64
	if len(value) != 9 {
		return out, fmt.Errorf("Couldn't unpack int64 value: wanted array of length 9, got %d", len(value))
	} else if value[0] != int64Type {
		return out, fmt.Errorf("Couldn't unpack int64 value: wanted type-byte %d, got %d", int64Type, value[0])
	}
	buf := bytes.NewReader(value[1:])
	binary.Read(buf, binary.BigEndian, &out)
	return out, nil
}

func packMd5HashValue(value [16]byte) []byte {
	buf := make([]byte, len(value)+1, len(value)+1)
	buf[0] = md5HashType
	for i := 0; i < len(value); i++ {
		buf[i+1] = value[i]
	}
	return buf
}

func unpackMd5HashValue(value []byte) ([16]byte, error) {
	out := [16]byte{}
	if len(value) != 17 {
		return out, fmt.Errorf("Couldn't unpack hash value: wanted array of length 17, got %d", len(value))
	} else if value[0] != md5HashType {
		return out, fmt.Errorf("Couldn't unpack hash value: wanted type-byte %d, got %d", hashType, value[0])
	}
	copy(out[:], value[1:])
	return out, nil
}

func packStringTimestampPair(name string, timestamp time.Time) []byte {
	// We actually store it as (type, nanosecond-timestamp, name) for easier unpacking
	buf := bytes.NewBuffer(make([]byte, 0, len(name)+9))
	buf.WriteByte(stringTimestampPairType)
	binary.Write(buf, binary.BigEndian, timestamp.UnixNano())
	buf.WriteString(name)
	return buf.Bytes()
}

func unpackStringTimestampPair(value []byte) (name string, timestamp time.Time, err error) {
	// it's packed as (type, nanosecond-timestamp, name)
	if len(value) < 9 {
		err = fmt.Errorf("Couldn't unpack string+timestamp value: wanted value of length >= 9, got %d", len(value))
		return
	} else if value[0] != stringTimestampPairType {
		err = fmt.Errorf("Couldn't unpack string+timestamp value: wanted type-byte %d, got %d",
			stringTimestampPairType, value[0])
		return
	}
	nano := int64(binary.BigEndian.Uint64(value[1:]))
	timestamp = time.Unix(nano/1000000000, nano%1000000000)
	name = string(value[9:])
	return
}

// Retrieve a container
func (cs *ContainerStore) GetContainer(partition uint32, account string, container string) (*ContainerInfo, bool, error) {
	var err error
	err = nil
	cinfo := defaultContainerInfo()
	cinfo.Account = account
	cinfo.Container = container

	containerHash := cs.hasher.HashContainerPath(account, container)

	lowerBound := containerAttributeKeyPrefix(partition, containerHash)
	upperBound := containerMetaKeyUpperBound(partition, containerHash)

	readopts := gorocksdb.NewDefaultReadOptions()
	readopts.SetVerifyChecksums(true)
	readopts.SetIterateUpperBound(upperBound)

	keysFound := 0
	iter := cs.db.NewIteratorCF(readopts, cs.metaCF)
	iter.Seek(lowerBound)
	for ; iter.Valid(); iter.Next() {
		keysFound++
		key := iter.Key().Data()
		value := iter.Value().Data()

		fmt.Printf("%v -> %v\n", key, value)

		// throw out the table prefix, partition, and container hash
		_, attrName, attrData, err := unpackAttributeKey(key)
		if err != nil {
			return nil, false, err
		}

		switch attrName {
		case accountType:
		case containerType:
		case createdAtType:
			cinfo.CreatedAt, err = unpackTimeValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack CreatedAt")
			}
		case putTimestampType:
			cinfo.PutTimestamp, err = unpackTimeValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack PutTimestamp")
			}
		case deleteTimestampType:
			cinfo.DeleteTimestamp, err = unpackTimeValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack DeleteTimestamp")
			}
		case reportedPutTimestampType:
			cinfo.ReportedPutTimestamp, err = unpackTimeValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack ReportedPutTimestamp")
			}
		case reportedDeleteTimestampType:
			cinfo.ReportedDeleteTimestamp, err = unpackTimeValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack ReportedDeleteTimestamp")
			}
		case reportedObjectCountType:
			cinfo.ReportedObjectCount, err = unpackUint64Value(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack ReportedObjectCount")
			}
		case reportedBytesUsedType:
			cinfo.ReportedBytesUsed, err = unpackUint64Value(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack ReportedBytesUsed")
			}
		case hashType:
			cinfo.Hash, err = unpackMd5HashValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack Hash")
			}
		case idType:
			cinfo.Id, err = unpackStringValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack Id")
			}
		case statusType:
			cinfo.Status, err = unpackStringValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack Status")
			}
		case statusChangedAtType:
			cinfo.StatusChangedAt, err = unpackTimeValue(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack StatusChangedAt")
			}
		case storagePolicyIndexType:
			cinfo.StoragePolicyIndex, err = unpackInt64Value(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack StoragePolicyIndex")
			}
		case containerSyncPoint1Type:
			cinfo.ContainerSyncPoint1, err = unpackInt64Value(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack ContainerSyncPoint1")
			}
		case containerSyncPoint2Type:
			cinfo.ContainerSyncPoint2, err = unpackInt64Value(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack ContainerSyncPoint2")
			}
		case reconcilerSyncPointType:
			cinfo.ReconcilerSyncPoint, err = unpackInt64Value(value)
			if err != nil {
				return nil, false, errors.Wrap(err, "Couldn't unpack ReconcilerSyncPoint")
			}
		case metadataType:
			metaName := string(attrData)
			metaValue, metaTime, err := unpackStringTimestampPair(value)
			if err != nil {
				return nil, false, errors.Wrapf(err, "Couldn't unpack metadatum %s", metaName)
			}
			if len(metaValue) > 0 {
				// Empty values are tombstones; we don't show those to callers
				cinfo.Metadata = append(cinfo.Metadata, MetadataItem{
					Name: metaName, Value: metaValue, Timestamp: metaTime})
			}
		default:
			return nil, false, fmt.Errorf("Unknown container attribute type %v", attrName)
		}

	}
	return cinfo, (keysFound > 0), err
}

// Create a container with the specified storage policy, metadata, and so on.
//
// Returns an error if the container exists.
func (cs *ContainerStore) CreateContainer(partition uint32, req CreateContainerRequest) error {
	containerHash := cs.hasher.HashContainerPath(req.Account, req.Container)

	// Any time we're possibly changing the storage policy, we should have an exclusive lock on the container.
	unlock := cs.lockManager.lockContainerForWrite(containerHash)
	defer unlock()

	cinfo, cexists, err := cs.GetContainer(partition, req.Account, req.Container)
	if err != nil {
		return err
	}
	if cexists && cinfo.PutTimestamp.After(cinfo.DeleteTimestamp) {
		// deleted container; we can resurrect it
		// TODO: resurrect it
	} else if !cexists {
		// no records for this container; we can create it
		batch := gorocksdb.NewWriteBatch()

		ci := defaultContainerInfo()
		id, err := uuid.NewRandom()
		if err != nil {
			return errors.Wrapf(err, "Failed to generate random UUID for container %s/%s", req.Account, req.Container)
		}
		ci.Account = req.Account
		ci.Container = req.Container
		ci.CreatedAt = req.Timestamp
		ci.PutTimestamp = req.Timestamp
		ci.Id = id.String()
		ci.StatusChangedAt = req.Timestamp
		ci.StoragePolicyIndex = req.StoragePolicyIndex

		prefix := containerAttributeKeyPrefix(partition, containerHash)

		// Container attributes: we'll have exactly one of each of these
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, accountType), packStringValue(ci.Account))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, containerType), packStringValue(ci.Container))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, createdAtType), packTimeValue(ci.CreatedAt))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, putTimestampType), packTimeValue(ci.CreatedAt))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, deleteTimestampType), packTimeValue(ci.DeleteTimestamp))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, reportedPutTimestampType), packTimeValue(ci.ReportedPutTimestamp))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, reportedDeleteTimestampType), packTimeValue(ci.ReportedDeleteTimestamp))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, reportedObjectCountType), packUint64Value(ci.ReportedObjectCount))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, reportedBytesUsedType), packUint64Value(ci.ReportedBytesUsed))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, hashType), packMd5HashValue(ci.Hash))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, idType), packStringValue(ci.Id))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, statusType), packStringValue(ci.Status))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, statusChangedAtType), packTimeValue(ci.StatusChangedAt))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, storagePolicyIndexType), packInt64Value(ci.StoragePolicyIndex))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, containerSyncPoint1Type), packInt64Value(ci.ContainerSyncPoint1))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, containerSyncPoint2Type), packInt64Value(ci.ContainerSyncPoint2))
		batch.PutCF(cs.metaCF, packAttributeKey(prefix, reconcilerSyncPointType), packInt64Value(ci.ReconcilerSyncPoint))

		for _, metaItem := range req.Metadata {
			// We deliberately ignore item.Timestamp in favor of req.Timestamp.
			batch.PutCF(cs.metaCF, packMetadataKey(prefix, metaItem.Name), packStringTimestampPair(metaItem.Value, req.Timestamp))
		}

		err = cs.db.Write(gorocksdb.NewDefaultWriteOptions(), batch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cs *ContainerStore) UpdateContainer(partition uint32, req UpdateContainerRequest) error {
	containerHash := cs.hasher.HashContainerPath(req.Account, req.Container)
	prefix := containerAttributeKeyPrefix(partition, containerHash)

	// We don't need much, but we do want to ensure that the container doesn't vanish out from under us while we're
	// updating the metadata. A shared lock is sufficient.
	unlock := cs.lockManager.lockContainerForRead(containerHash)
	defer unlock()

	_, cexists, err := cs.GetContainer(partition, req.Account, req.Container)
	if err != nil {
		return err
	}
	if !cexists {
		return &ContainerNotFoundError{Account: req.Account, Container: req.Container}
	}

	// We don't look at existing metadata at all because we may receive updates out of order. Instead, we just take each
	// new metadatum and Merge() it into the database, where our merge operator will ensure that the last write wins.
	batch := gorocksdb.NewWriteBatch()
	for _, metaItem := range req.Metadata {
		batch.MergeCF(cs.metaCF, packMetadataKey(prefix, metaItem.Name), packStringTimestampPair(metaItem.Value, req.Timestamp))
	}
	return cs.db.Write(gorocksdb.NewDefaultWriteOptions(), batch)
}

// type UpdateContainerRequest struct {
// 	Account   string
// 	Container string
// 	Timestamp time.Time
// 	Metadata  []MetadataItem
// }
