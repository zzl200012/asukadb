// Created on 2021/3/23 by @zzl
package memtable

import (
	"asukadb/common"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func Test_MemTable(t *testing.T) {
	memTable := New()
	memTable.Add(1234567, common.TypeValue, []byte("zzl"), []byte("1209"))
	for i := 0; i < 10; i++ {
		go memTable.Add(rand.Uint64(), common.TypeValue, []byte(string(rune(i))), []byte(string(rune(rand.Int()))))
	}
	time.Sleep(500 * time.Millisecond)
	value, _ := memTable.Get([]byte("zzl"))
	if string(value) != "1209" {
		t.Fail()
	}
	memTable.Add(math.MaxUint64, common.TypeDeletion, []byte(string(rune(3))), nil)
	value, _ = memTable.Get([]byte(string(rune(3))))
	if string(value) != "" {
		t.Fail()
	}
	fmt.Println(memTable.ApproximateMemoryUsage())
}