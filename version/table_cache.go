// Created on 2021/3/25 by @zzl
package version

import (
	"asukadb/common"
	"asukadb/lru"
	"asukadb/sstable"
	"sync"
)

type TableCache struct {
	mu sync.Mutex
	cache *lru.Cache
	dbName string
}

func NewTableCache(dbName string) *TableCache {
	var tableCache TableCache
	tableCache.cache, _ = lru.NewCache(common.MaxOpenFiles - common.NumNonTableCacheFiles, nil)
	tableCache.dbName = dbName
	return &tableCache
}

func (tableCache *TableCache) NewSSTIterator(fileNum uint64) *sstable.Iterator {
	table, err := tableCache.getTable(fileNum)
	if err != nil {
		return nil
	}
	return table.NewIterator()
}

func (tableCache *TableCache) Get(fileNum uint64, key []byte) ([]byte, error) {
	table, err := tableCache.getTable(fileNum)
	if err != nil {
		return nil, err
	}
	return table.Get(key)
}

func (tableCache *TableCache) Evict(fileNum uint64) {
	tableCache.mu.Lock()
	defer tableCache.mu.Unlock()
	tableCache.cache.Remove(fileNum)
}

func (tableCache *TableCache) getTable(fileNum uint64) (*sstable.SsTable, error) {
	tableCache.mu.Lock()
	defer tableCache.mu.Unlock()

	table, ok := tableCache.cache.Get(fileNum)
	if ok {
		return table.(*sstable.SsTable), nil
	} else {
		newTable, err := sstable.Open(common.GetTableFileName(tableCache.dbName, fileNum))
		tableCache.cache.Add(fileNum, newTable)
		return newTable, err
	}
}
