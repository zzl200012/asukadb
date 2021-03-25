// Created on 2021/3/24 by @zzl
package sstable

import (
	"asukadb/common"
	"testing"
)

func Test_SsTable(t *testing.T) {
	tableName := common.GetTableFileName("asuka", 000)
	println(tableName)
	builder := NewTableBuilder(tableName)
	item := common.NewInternalKey(1, common.TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = common.NewInternalKey(2, common.TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = common.NewInternalKey(3, common.TypeValue, []byte("125"), []byte("0245"))
	builder.Add(item)
	builder.Finish()

	table, err := Open(tableName)
	if err != nil {
		t.Fail()
	}
	it := table.NewIterator()
	it.Seek([]byte("1244"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "125" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
}