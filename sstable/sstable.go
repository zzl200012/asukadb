// Created on 2021/3/24 by @zzl
package sstable

import (
	"asukadb/common"
	"asukadb/sstable/block"
	"io"
	"os"
)

type SsTable struct {
	indexBlock *block.Block
	metaIndexBlock *block.Block
	footer     Footer
	file       *os.File
}

func Open(fileName string) (*SsTable, error) {
	var table SsTable
	var err error
	table.file, err = os.Open(fileName)
	if err != nil {
		return nil, err
	}
	stat, _ := table.file.Stat()
	// Read the footer block
	footerSize := int64(table.footer.Size())
	if stat.Size() < footerSize {
		return nil, common.ErrTableFileTooShort
	}

	_, err = table.file.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	err = table.footer.DecodeFrom(table.file)
	if err != nil {
		return nil, err
	}
	// Read the index block and meta index block
	table.indexBlock = table.readBlock(table.footer.IndexHandle)
	//table.metaIndexBlock = table.readBlock(table.footer.MetaIndexHandle)
	return &table, nil
}

func (table *SsTable) NewIterator() *Iterator {
	var it Iterator
	it.table = table
	it.indexIter = table.indexBlock.NewIterator()
	return &it
}

func (table *SsTable) Get(key []byte) ([]byte, error) {
	it := table.NewIterator()
	it.Seek(key)
	if it.Valid() {
		internalKey := it.InternalKey()
		if common.UserKeyComparator(key, internalKey.UserKey) == 0 {
			// matched
			if internalKey.Type == common.TypeValue {
				return internalKey.UserValue, nil
			} else {
				return nil, common.ErrDeletion
			}
		}
	}
	return nil, common.ErrNotFound
}

func (table *SsTable) readBlock(blockHandle BlockHandle) *block.Block {
	p := make([]byte, blockHandle.Size)
	n, err := table.file.ReadAt(p, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}

	return block.New(p)
}