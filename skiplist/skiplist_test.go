// Created on 2021/3/20 by @zzl
package skiplist

import (
	"asukadb/common"
	"math/rand"
	"testing"
	"time"
)

func Test_Insert(t *testing.T) {
	list := New(common.IntComparator)
	ans := make([]int, 10)
	for i := 0; i < 10; i++ {
		num := rand.Int() % 10
		ans[i] = num
		go list.Insert(num)
	}
	time.Sleep(500 * time.Millisecond)
	it := list.NewIterator()
	index := 0
	prev := -1
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if common.IntComparator(it.Key(), prev) < 0 {
			t.Fail()
			prev = it.Key().(int)
		}
		index++
	}
}

