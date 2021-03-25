// Created on 2021/3/25 by @zzl
package version

import log "github.com/sirupsen/logrus"

type Compaction struct {
	level  int
	inputs [2][]*FileMetaData
}

// Is this a trivial compaction that can be implemented by just
// moving a single input file to the next level (no merging or splitting)
func (c *Compaction) isTrivialMove() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

func (c *Compaction) Log() {
	log.Infof("Compaction, level:%d", c.level)
	for i := 0; i < len(c.inputs[0]); i++ {
		log.Infof("inputs[0]: %d", c.inputs[0][i].number)
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		log.Infof("inputs[1]: %d", c.inputs[1][i].number)
	}
}
