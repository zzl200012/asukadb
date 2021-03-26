// Created on 2021/3/26 by @zzl
package db

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func TestDB(t *testing.T) {
	db := Open("ASUKA")
	for i := 0; i < 99999; i++ {
		db.Put([]byte(strconv.FormatUint(r.Uint64(), 10)), []byte(strconv.FormatUint(r.Uint64(), 10)))
	}
	db.Put([]byte("zzl"), []byte("1209"))
	value, err := db.Get([]byte("zzl"))
	if err != nil {
		t.Fail()
	}
	if string(value) != "1209" {
		t.Fail()
	}
	db.Close()

	db_ := Open("ASUKA")
	value, err = db_.Get([]byte("zzl"))
	if err != nil {
		t.Fail()
	}
	if string(value) != "1209" {
		t.Fail()
	}
	db_.Close()
}
