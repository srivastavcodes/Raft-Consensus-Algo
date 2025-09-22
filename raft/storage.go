package raft

import "sync"

// Storage is an interface implemented by stable storage providers.
type Storage interface {
	// HasData returns true if any (Set)s were made on this Storage.
	HasData() bool

	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	mu    sync.Mutex
	store map[string][]byte
}

func NewMapStorage() *MapStorage {
	return &MapStorage{
		store: make(map[string][]byte),
	}
}

func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.store) > 0
}

func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	val, ok := ms.store[key]
	return val, ok
}

func (ms *MapStorage) Set(key string, value []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.store[key] = value
}
