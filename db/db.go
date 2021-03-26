// Created on 2021/3/24 by @zzl
package db

import (
	"asukadb/common"
	"asukadb/memtable"
	"asukadb/version"
	"sync"
)

type DB struct {
	mu                           sync.Mutex
	backgroundWorkFinishedSignal *sync.Cond
	name                         string
	seq                          uint64
	compactionScheduled          bool
	memTable                     *memtable.MemTable
	iMemTable                    *memtable.MemTable
	currentVersion               *version.Version
}

// Standard APIs for AsukaDB

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	mm := db.memTable
	imm := db.iMemTable
	curr := db.currentVersion
	db.mu.Unlock()

	// search from memtable first
	value,err := mm.Get(key)
	if err != common.ErrNotFound {
		return value, err
	}

	// then search from immutable memtable
	if imm != nil {
		value, err = imm.Get(key)
		if err != common.ErrNotFound {
			return value, err
		}
	}

	// finally search from sstable, if not found, then we don't contain such a key
	return curr.Get(key)
}

func (db *DB) Put(key, value []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}

	// todo: add log

	db.memTable.Add(seq, common.TypeValue, key, value)
	return nil
}

func (db *DB) Del(key []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}

	// todo: add log

	db.memTable.Add(seq, common.TypeDeletion, key, nil)
	return nil
}

func Open(dbName string) *DB {
	var db DB
	db.name = dbName
	db.memTable = memtable.New()
	db.backgroundWorkFinishedSignal = sync.NewCond(&db.mu)
	fileNum := db.ReadCurrentFile()
	if fileNum > 0 {
		v, err := version.LoadFromLocal(dbName, fileNum)
		if err != nil {
			return nil
		}
		db.currentVersion = v
	} else {
		db.currentVersion = version.New(dbName)
	}
	return &db
}

func (db *DB) Close() {
	db.mu.Lock()
	for db.compactionScheduled {
		db.backgroundWorkFinishedSignal.Wait()
	}
	db.mu.Unlock()
}

