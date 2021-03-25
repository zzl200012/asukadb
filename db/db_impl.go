// Created on 2021/3/24 by @zzl
package db

import (
	"asukadb/common"
	"asukadb/memtable"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func (db *DB) makeRoomForWrite() (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for {
		if db.currentVersion.NumLevelFiles(0) >= common.L0SlowdownWritesTrigger {
			// We are getting close to hitting a hard limit on the number of
			// L0 files.  Rather than delaying a single write by several
			// seconds when we hit the hard limit, start delaying each
			// individual write by 1ms to reduce latency variance.  Also,
			// this delay hands over some CPU to the compaction thread in
			// case it is sharing the same core as the writer.
			db.mu.Unlock()
			time.Sleep(time.Millisecond)
			db.mu.Lock()
		} else if db.memTable.ApproximateMemoryUsage() <= common.WriteBufferSize {
			// There is room in current memtable
			break
		} else if db.iMemTable != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			db.backgroundWorkFinishedSignal.Wait()
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			db.iMemTable = db.memTable
			db.memTable = memtable.New()
			// todo: switch log file
			db.maybeScheduleCompaction()
		}
	}

	return db.currentVersion.NextSeq(), nil
}

// REQUIRES: db.mu.Lock()
func (db *DB) maybeScheduleCompaction() {
	if db.compactionScheduled {
		// Already scheduled
		return
	}
	db.compactionScheduled = true
	go db.backgroundCall()
}

func (db *DB) backgroundCall() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.backgroundCompaction()
	db.compactionScheduled = false
	db.backgroundWorkFinishedSignal.Broadcast()
}

// REQUIRES: db.mu.Lock()
func (db *DB) backgroundCompaction() {
	base := db.currentVersion.Copy()
	imm := db.iMemTable

	// Release mutex while we're actually doing the compaction work
	db.mu.Unlock()

	// Minor compaction
	if imm != nil {
		// Save the contents of the memtable as a new Table
		base.WriteLevel0Table(imm)
	}

	// Major compaction
	for base.DoCompactionWork() {
		base.Log()
	}

	descriptorNumber, _ := base.Save()
	db.SetCurrentFile(descriptorNumber)
	db.mu.Lock()
	db.iMemTable = nil
	db.currentVersion = base
}

func (db *DB) SetCurrentFile(descriptorNumber uint64) {
	tmp := common.GetTempFileName(db.name, descriptorNumber)
	ioutil.WriteFile(tmp, []byte(fmt.Sprintf("%d", descriptorNumber)), 0600)
	os.Rename(tmp, common.GetCurrentFileName(db.name))
}

func (db *DB) ReadCurrentFile() uint64 {
	b, err := ioutil.ReadFile(common.GetCurrentFileName(db.name))
	if err != nil {
		return 0
	}
	descriptorNumber, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return descriptorNumber
}
