// Created on 2021/3/22 by @zzl
package common

type Comparator func(a, b interface{}) int

func IntComparator(a, b interface{}) int {
	aInt := a.(int)
	bInt := b.(int)
	return aInt - bInt
}
