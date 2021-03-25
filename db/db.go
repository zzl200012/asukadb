// Created on 2021/3/24 by @zzl
package db

import (
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
	panic("unimplemented")
}

func (db *DB) Put(key, value []byte) error {
	panic("unimplemented")
}

func (db *DB) Del(key []byte) error {
	panic("unimplemented")
}
