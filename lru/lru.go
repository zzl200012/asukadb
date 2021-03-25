// Created on 2021/3/25 by @zzl
package lru

import (
	"container/list"
	"errors"
)

type EvictCallback func(key interface{}, value interface{})

// Not thread-safe
type Cache struct {
	size      int
	evictList *list.List
	mapping   map[interface{}]*list.Element
	onEvict   EvictCallback
}

func NewCache(size int, onEvict EvictCallback) (*Cache, error) {
	if size <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	c := &Cache{
		size:      size,
		evictList: list.New(),
		mapping:     make(map[interface{}]*list.Element),
		onEvict:   onEvict,
	}
	return c, nil
}

type Entry struct {
	key interface{}
	value interface{}
}

// Returns true if eviction occurred.
func (c *Cache) Add(key, value interface{}) bool {
	// Check the existence
	if entry, ok := c.mapping[key]; ok {
		c.evictList.MoveToFront(entry)
		entry.Value.(*Entry).value = value
		return false
	}

	// Add new entry
	entry := Entry{
		key:   key,
		value: value,
	}
	e := c.evictList.PushFront(entry)
	c.mapping[key] = e

	// Check the eviction
	if c.evictList.Len() > c.size {
		c.removeOldest()
		return true
	}

	return false
}

// Returns value for the key, false if not found.
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	if entry, ok := c.mapping[key]; ok {
		c.evictList.MoveToFront(entry)
		if entry.Value.(*Entry) != nil {
			return entry.Value.(*Entry).value, true
		} else {
			return nil, false
		}
	}
	return nil, false
}

// Returns true if removed successfully
func (c *Cache) Remove(key interface{}) bool {
	if entry, ok := c.mapping[key]; ok {
		c.removeElement(entry)
		return true
	}
	return false
}

// Removes the oldest item from the cache.
func (c *Cache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// Removes a given list element from the cache
func (c *Cache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*Entry)
	delete(c.mapping, kv.key)
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
