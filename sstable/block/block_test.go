// Created on 2021/3/24 by @zzl
package block

import (
	"asukadb/common"
	"testing"
)

func Test_Block(t *testing.T) {
	var builder BlockBuilder

	item := common.NewInternalKey(1, common.TypeValue, []byte("aaa"), []byte("123"))
	builder.Add(item)
	item = common.NewInternalKey(2, common.TypeValue, []byte("bbb"), []byte("234"))
	builder.Add(item)
	item = common.NewInternalKey(3, common.TypeValue, []byte("ccc"), []byte("456"))
	builder.Add(item)
	p := builder.Finish()

	block := New(p)
	it := block.NewIterator()

	it.Seek([]byte("aaa"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "123" {
			t.Fail()
		}

	} else {
		t.Fail()
	}
}
