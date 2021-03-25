// Created on 2021/3/25 by @zzl
package version

import (
	"asukadb/common"
	"asukadb/memtable"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sort"
)

type Version struct {
	tableCache     *TableCache
	nextFileNumber uint64
	seq            uint64
	files          [common.NumLevels][]*FileMetaData
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
	fileName := common.GetDescriptorFileName(dbName, num)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	v := New(dbName)
	err = v.DecodeFrom(file)
	return v, err
}

func (v *Version) Save() (uint64, error) {
	num := v.nextFileNumber
	fileName := common.GetDescriptorFileName(v.tableCache.dbName, num)
	v.nextFileNumber++
	file, err := os.Create(fileName)
	if err != nil {
		return num, err
	}
	defer file.Close()
	return num, v.EncodeTo(file)
}

// Deep copy a version
func (v *Version) Copy() *Version {
	var c Version

	c.tableCache = v.tableCache
	c.nextFileNumber = v.nextFileNumber
	c.seq = v.seq
	for level := 0; level < common.NumLevels; level++ {
		c.files[level] = make([]*FileMetaData, len(v.files[level]))
		copy(c.files[level], v.files[level])
	}
	return &c
}

func (v *Version) NextSeq() uint64 {
	v.seq++
	return v.seq
}

func (v *Version) NumLevelFiles(level int) int {
	return len(v.files[level])
}

func (v *Version) Get(key []byte) ([]byte, error) {
	// We can search level-by-level since entries never hop across
	// levels.  Therefore we are guaranteed that if we find data
	// in a smaller level, later levels are irrelevant.
	var tmp []*FileMetaData
	var tmp2 [1]*FileMetaData

	var files []*FileMetaData

	for level := 0; level < common.NumLevels; level++ {
		numFiles := v.NumLevelFiles(level)
		if numFiles == 0 {
			continue
		}

		if level == 0 {
			// Level-0 files may overlap each other.  Find all files that
			// overlap user_key and process them in order from newest to oldest.
			for i := 0; i < numFiles; i++ {
				f := v.files[level][i]
				if common.UserKeyComparator(key, f.smallest.UserKey) >= 0 && common.UserKeyComparator(key, f.largest.UserKey) <= 0 {
					tmp = append(tmp, f)
				}
			}
			if len(tmp) == 0 {
				continue
			}
			sort.Slice(tmp, func(i, j int) bool { return tmp[i].number > tmp[j].number })
			files = tmp
			numFiles = len(tmp)
		} else {
			// Binary search to find earliest index whose largest key >= ikey.
			index := v.findFile(v.files[level], key)
			if index >= numFiles {
				files = nil
				numFiles = 0
			} else {
				tmp2[0] = v.files[level][index]
				if common.UserKeyComparator(key, tmp2[0].smallest.UserKey) < 0 {
					// All of "tmp2" is past any data for user_key
					files = nil
					numFiles = 0
				} else {
					files = tmp2[:]
					numFiles = 1
				}
			}
		}
		for i := 0; i < numFiles; i++ {
			f := files[i]
			value, err := v.tableCache.Get(f.number, key)
			if err != common.ErrNotFound {
				return value, err
			}
		}
	}
	return nil, common.ErrNotFound
}

func (v *Version) findFile(files []*FileMetaData, key []byte) int {
	left := 0
	right := len(files)
	for left < right {
		mid := (left + right) / 2
		f := files[mid]
		if common.UserKeyComparator(f.largest.UserValue, key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return right
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
		v.files[level] = make([]*FileMetaData, numFiles)
		for i := 0; i < int(numFiles); i++ {
			var meta FileMetaData
			meta.DecodeFrom(r)
			v.files[level][i] = &meta
		}
	}
	return nil
}

// Compaction related

func (v *Version) WriteLevel0Table(imm *memtable.MemTable) {
	panic("")
}

func (v *Version) DoCompactionWork() bool {
	panic("")
}

// Add the specified file at the specified level.
func (v *Version) addFile(level int, meta *FileMetaData) {
	panic("")
}

// Delete the specified "file" from the specified "level".
func (v *Version) deleteFile(level int, meta *FileMetaData) {
	panic("")
}

// Returns true iff some file in the specified level overlaps
func (v *Version) overlapInLevel(level int, smallest, largest []byte) bool {
	panic("")
}

func (v *Version) getInputIterator(c *Compaction) *MergingIterator {
	panic("")
}

func (v *Version) pickCompaction() *Compaction {
	panic("")
}

func (v *Version) pickCompactionLevel() int {
	panic("")
}

// more func


