// Created on 2021/3/22 by @zzl
package common

import "errors"

var (
	ErrNotFound          = errors.New("not found")
	ErrDeletion          = errors.New("deletion")
	ErrTableFileMagic    = errors.New("not an sstable (bad magic number)")
	ErrTableFileTooShort = errors.New("file is too short to be an sstable")
)
