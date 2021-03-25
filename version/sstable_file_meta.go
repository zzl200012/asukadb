// Created on 2021/3/24 by @zzl
package version

import "asukadb/common"

type SSTFileMeta struct {
	allowSeeks uint64
	number     uint64
	fileSize   uint64
	smallest   *common.InternalKey
	largest    *common.InternalKey
}

