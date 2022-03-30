package service

import (
	"sync"
)

// RingList is a circular list of strings with a set capacity.
// This structure uses mutexes for adding and searching, so it
// should be safe to share across goroutines.
type RingList struct {
	capacity int
	index    int
	items    []string
	mutex    *sync.RWMutex
}

// NewRingList creates a new RingList with the specified capacity.
func NewRingList(capacity int) *RingList {
	return &RingList{
		capacity: capacity,
		index:    0,
		items:    make([]string, capacity),
		mutex:    &sync.RWMutex{},
	}
}

// Add adds an item to the Ringlist. If capacity is ten, then
// the eleventh item you add overwrites item #1.
func (list *RingList) Add(item string) {
	list.mutex.Lock()
	list.index += 1
	if list.index == list.capacity {
		list.index = 0
	}
	list.items[list.index] = item
	list.mutex.Unlock()
}

// Contains returns true if the item is in the RingList.
func (list *RingList) Contains(item string) bool {
	exists := false
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	for _, value := range list.items {
		if value == item {
			exists = true
			break
		}
	}
	return exists
}

// Del deletes all instances of the item from the list,
// replacing those instances with an empty string.
func (list *RingList) Del(item string) {
	if item == "" {
		return
	}
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	for i, value := range list.items {
		if value == item {
			list.items[i] = ""
		}
	}
}

// Items returns the non-empty items in this ring list.
func (list *RingList) Items() []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	items := make([]string, 0)
	for _, item := range list.items {
		if item != "" {
			items = append(items, item)
		}
	}
	return items
}
