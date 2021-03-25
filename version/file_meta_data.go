// Created on 2021/3/24 by @zzl
package version

import (
	"asukadb/common"
	"encoding/binary"
	"io"
)

type FileMetaData struct {
	allowSeeks uint64
	number     uint64
	fileSize   uint64  // File size in bytes
	smallest   *common.InternalKey  // Smallest internal key served by table
	largest    *common.InternalKey  // Largest internal key served by table
}

func (meta *FileMetaData) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, meta.allowSeeks)
	binary.Write(w, binary.LittleEndian, meta.fileSize)
	binary.Write(w, binary.LittleEndian, meta.number)
	meta.smallest.EncodeTo(w)
	meta.largest.EncodeTo(w)
	return nil
}

func (meta *FileMetaData) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &meta.allowSeeks)
	binary.Read(r, binary.LittleEndian, &meta.fileSize)
	binary.Read(r, binary.LittleEndian, &meta.number)
	meta.smallest = new(common.InternalKey)
	meta.smallest.DecodeFrom(r)
	meta.largest = new(common.InternalKey)
	meta.largest.DecodeFrom(r)
	return nil
}

