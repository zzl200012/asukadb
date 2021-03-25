// Created on 2021/3/23 by @zzl
package block

import (
	"asukadb/common"
	"bytes"
	"encoding/binary"
)

type Block struct {
	items []common.InternalKey
}

func New(p []byte) *Block {
	var block Block
	data := bytes.NewBuffer(p)
	counter := binary.LittleEndian.Uint32(p[len(p)-4:])

	for i := uint32(0); i < counter; i++ {
		var item common.InternalKey
		err := item.DecodeFrom(data)
		if err != nil {
			return nil
		}
		block.items = append(block.items, item)
	}

	return &block
}

func (block *Block) NewIterator() *Iterator {
	return &Iterator{block: block}
}

// Iterator

type Iterator struct {
	block *Block
	index int
}

// Returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.index >= 0 && it.index < len(it.block.items)
}

func (it *Iterator) InternalKey() *common.InternalKey {
	return &it.block.items[it.index]
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.index++
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.index--
}

// Advance to the first entry with a key >= target
func (it *Iterator) Seek(target interface{}) {
	// binary search
	left := 0
	right := len(it.block.items) - 1
	for left < right {
		mid := (left + right) / 2
		if common.UserKeyComparator(it.block.items[mid].UserKey, target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left == len(it.block.items)-1 {
		if common.UserKeyComparator(it.block.items[left].UserKey, target) < 0 {
			// not found
			left++
		}
	}
	it.index = left
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToFirst() {
	it.index = 0
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToLast() {
	if len(it.block.items) > 0 {
		it.index = len(it.block.items) - 1
	}
}