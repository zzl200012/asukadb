// Created on 2021/3/24 by @zzl
package filter

import (
	"hash"
	"hash/fnv"
	"math"
)

// The standard bloom filter, which allows adding of
// elements, and checking for their existence
type BloomFilter struct {
	bitmap []bool      // The bloom-filter bitmap
	k      int         // Number of hash functions
	n      int         // Number of elements in the filter
	m      int         // Size of the bloom filter
	hashfn hash.Hash64 // The hash function
}

// Returns a new BloomFilter object, if you pass the
// number of Hash Functions to use and the maximum
// size of the Bloom Filter
func NewBloomFilter(numHashFuncs, bfSize int) *BloomFilter {
	bf := new(BloomFilter)
	bf.bitmap = make([]bool, bfSize)
	bf.k, bf.m = numHashFuncs, bfSize
	bf.n = 0
	bf.hashfn = fnv.New64()
	return bf
}

func (bf *BloomFilter) getHash(b []byte) (uint32, uint32) {
	bf.hashfn.Reset()
	bf.hashfn.Write(b)
	hash64 := bf.hashfn.Sum64()
	h1 := uint32(hash64 & ((1 << 32) - 1))
	h2 := uint32(hash64 >> 32)
	return h1, h2
}

// Adds an element (in byte-array form) to the Bloom Filter
func (bf *BloomFilter) Add(e []byte) {
	h1, h2 := bf.getHash(e)
	for i := 0; i < bf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(bf.m)
		bf.bitmap[ind] = true
	}
	bf.n++
}

// Checks if an element (in byte-array form) exists in the
// Bloom Filter
func (bf *BloomFilter) Check(x []byte) bool {
	h1, h2 := bf.getHash(x)
	result := true
	for i := 0; i < bf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(bf.m)
		result = result && bf.bitmap[ind]
	}
	return result
}

// Returns the current False Positive Rate of the Bloom Filter
func (bf *BloomFilter) FalsePositiveRate() float64 {
	return math.Pow(1 - math.Exp(-float64(bf.k*bf.n)/
			float64(bf.m)), float64(bf.k))
}


