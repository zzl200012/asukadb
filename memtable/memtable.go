// Created on 2021/3/22 by @zzl
package memtable

import (
	"asukadb/common"
	"asukadb/skiplist"
)

type MemTable struct {
	table *skiplist.SkipList
	memoryUsage uint64
}

func New() *MemTable {
	var memTable MemTable
	memTable.table = skiplist.New(common.InternalKeyComparator)
	return &memTable
}

func (memTable *MemTable) NewIterator() *Iterator {
	return &Iterator{listIter: memTable.table.NewIterator()}
}

func (memTable *MemTable) Add(seq uint64, valueType common.ValueType, key, value []byte) {
	internalKey := common.NewInternalKey(seq, valueType, key, value)

	memTable.memoryUsage += uint64(16 + len(key) + len(value))
	memTable.table.Insert(internalKey)
}

func (memTable *MemTable) Get(key []byte) ([]byte, error) {
	lookupKey := common.LookupKey(key)
	it := memTable.table.NewIterator()
	it.Seek(lookupKey)
	if it.Valid() {
		// Check that it belongs to same user key.  We do not check the
		// sequence number since the Seek() call above should have skipped
		// all entries with overly large sequence numbers.
		internalKey := it.Key().(*common.InternalKey)
		if common.UserKeyComparator(internalKey.UserKey, key) == 0 {
			// Correct user key
			if internalKey.Type == common.TypeValue {
				return internalKey.UserValue, nil
			} else {
				return nil, common.ErrDeletion
			}
		}
	}
	return nil, common.ErrNotFound
}

func (memTable *MemTable) ApproximateMemoryUsage() uint64 {
	return memTable.memoryUsage
}

// Iterator

type Iterator struct {
	listIter *skiplist.Iterator
}

// Returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.listIter.Valid()
}

func (it *Iterator) InternalKey() *common.InternalKey {
	return it.listIter.Key().(*common.InternalKey)
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.listIter.Next()
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.listIter.Prev()
}

// Advance to the first entry with a key >= target
func (it *Iterator) Seek(target interface{}) {
	it.listIter.Seek(target)
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToFirst() {
	it.listIter.SeekToFirst()
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToLast() {
	it.listIter.SeekToLast()
}