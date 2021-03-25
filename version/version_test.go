// Created on 2021/3/25 by @zzl
package version

import (
	"asukadb/common"
	"asukadb/memtable"
	"fmt"
	"testing"
)

func Test_Version_Get(t *testing.T) {
	v := New("./temp_ver_0")
	var f FileMetaData
	f.number = 123
	f.smallest = common.NewInternalKey(1, common.TypeValue, []byte("123"), nil)
	f.largest = common.NewInternalKey(1, common.TypeValue, []byte("125"), nil)
	v.files[0] = append(v.files[0], &f)

	value, err := v.Get([]byte("125"))
	fmt.Println(err, value)
}

func Test_Version_Load(t *testing.T) {
	v := New("./temp_ver_1")
	memTable := memtable.New()
	memTable.Add(1234567, common.TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	v.WriteLevel0Table(memTable)
	n, _ := v.Save()
	fmt.Println(v)

	v2, _ := LoadFromLocal("./temp_ver_1", n)
	fmt.Println(v2)
	value, err := v2.Get([]byte("aadsa34a"))
	fmt.Println(err, value)
}