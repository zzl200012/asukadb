// Created on 2021/3/22 by @zzl
package common

import "fmt"

const (
	// Level-0 compaction is started when we hit this many files.
	L0CompactionTrigger     = 4

	// Soft limit on number of level-0 files.  We slow down writes at this point.
	L0SlowdownWritesTrigger = 8

	// Amount of data to build up in memory (backed by an unsorted log
	// on disk) before converting to a sorted on-disk file.
	WriteBufferSize         = 4 << 20

	NumLevels               = 7


	MaxOpenFiles            = 1000
	NumNonTableCacheFiles   = 10
	MaxMemCompactLevel      = 2
	MaxFileSize             = 2 << 20
)

func getFilename(dbname string, number uint64, suffix string) string {
	return fmt.Sprintf("./%s-%06d.%s", dbname, number, suffix)
}

func GetTableFileName(dbname string, number uint64) string {
	return getFilename(dbname, number, "sst")
}

func GetDescriptorFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s-MANIFEST-%06d", dbname, number)
}

func GetCurrentFileName(dbname string) string {
	return dbname + "-CURRENT"
}
func GetTempFileName(dbname string, number uint64) string {
	return getFilename(dbname, number, "tmp")
}
