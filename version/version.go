// Created on 2021/3/25 by @zzl
package version

import (
	"asukadb/common"
	"asukadb/memtable"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"io"
)

type Version struct {
	tableCache     *TableCache
	nextFileNumber uint64
	seq            uint64
	files          [common.NumLevels][]*SSTFileMeta
	// Per-level key at which the next compaction at that level should start.
	// Either an empty string, or a valid InternalKey.
	compactPointer [common.NumLevels]*common.InternalKey
}

func New(dbName string) *Version {
	var v Version
	v.tableCache = NewTableCache(dbName)
	v.nextFileNumber = 1
	return &v
}

func LoadFromLocal(dbName string, num uint64) (*Version, error) {
	panic("")
}

func (v *Version) Save() (uint64, error) {
	panic("")
}

// Deep copy a version
func (v *Version) Copy() *Version {
	panic("")
}

func (v *Version) NextSeq() uint64 {
	panic("")
}

func (v *Version) NumLevelFiles(l int) int {
	panic("")
}

func (v *Version) Get(key []byte) ([]byte, error) {
	panic("")
}

func (v *Version) WriteLevel0Table(imm *memtable.MemTable) {
	panic("")
}

func (v *Version) DoCompactionWork() bool {
	panic("")
}

func (v *Version) Log() {
	for level := 0; level < common.NumLevels; level++ {
		for i := 0; i < len(v.files[level]); i++ {
			log.Infof("version[%d]: %d", level, v.files[level][i].number)
		}
	}
}

func (v *Version) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, v.nextFileNumber)
	binary.Write(w, binary.LittleEndian, v.seq)
	for level := 0; level < common.NumLevels; level++ {
		numFiles := len(v.files[level])
		binary.Write(w, binary.LittleEndian, int32(numFiles))

		for i := 0; i < numFiles; i++ {
			v.files[level][i].EncodeTo(w)
		}
	}
	return nil
}

func (v *Version) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &v.nextFileNumber)
	binary.Read(r, binary.LittleEndian, &v.seq)
	var numFiles int32
	for level := 0; level < common.NumLevels; level++ {
		binary.Read(r, binary.LittleEndian, &numFiles)
		v.files[level] = make([]*SSTFileMeta, numFiles)
		for i := 0; i < int(numFiles); i++ {
			var meta SSTFileMeta
			meta.DecodeFrom(r)
			v.files[level][i] = &meta
		}
	}
	return nil
}