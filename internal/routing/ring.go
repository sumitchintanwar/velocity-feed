package routing

import (
	"sort"
	"strconv"
	"sync"

	"github.com/sumit/rtmds/internal/hashing"
)

// ConsistentHashRing manages the distribution of partitions to physical nodes.
type ConsistentHashRing struct {
	mu          sync.RWMutex
	vnodes      int
	keys        []uint64
	nodes       map[uint64]string // maps hash key to physical node ID
	nodePresent map[string]bool   // tracks which physical nodes are active
}

// NewConsistentHashRing creates a new ring with the specified number of virtual nodes per physical node.
func NewConsistentHashRing(vnodes int) *ConsistentHashRing {
	return &ConsistentHashRing{
		vnodes:      vnodes,
		keys:        make([]uint64, 0),
		nodes:       make(map[uint64]string),
		nodePresent: make(map[string]bool),
	}
}

// AddNode adds a physical node to the ring, generating `vnodes` virtual nodes.
func (r *ConsistentHashRing) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If node is already present, do nothing to avoid duplicate vnodes
	if r.nodePresent[node] {
		return
	}
	r.nodePresent[node] = true

	for i := 0; i < r.vnodes; i++ {
		// Generate a virtual node string (e.g., "node1#0", "node1#1")
		vnodeKey := node + "#" + strconv.Itoa(i)
		hashKey := hashing.HashString(vnodeKey)

		r.keys = append(r.keys, hashKey)
		r.nodes[hashKey] = node
	}

	// Re-sort the keys slice to maintain binary search capability
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// RemoveNode safely removes a physical node and all of its virtual nodes from the ring.
func (r *ConsistentHashRing) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodePresent[node] {
		return
	}
	delete(r.nodePresent, node)

	// Filter out the keys belonging to this node
	newKeys := make([]uint64, 0, len(r.keys)-r.vnodes)
	for _, k := range r.keys {
		if r.nodes[k] == node {
			delete(r.nodes, k)
		} else {
			newKeys = append(newKeys, k)
		}
	}
	r.keys = newKeys
}

// Lookup finds the appropriate physical node for a given string key (e.g., symbol or partition).
func (r *ConsistentHashRing) Lookup(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return ""
	}

	hashKey := hashing.HashString(key)

	// Binary search for the first virtual node that is >= the hashKey
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hashKey
	})

	// Wrap around to the start of the ring if we went past the last virtual node
	if idx == len(r.keys) {
		idx = 0
	}

	return r.nodes[r.keys[idx]]
}

// LookupPartition finds the appropriate physical node for a numeric partition ID.
// This avoids allocating strings if the router works directly with partition integers.
func (r *ConsistentHashRing) LookupPartition(partitionID uint32) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return ""
	}

	// We use an 8-byte buffer to avoid heap allocations during hashing
	var buf [8]byte
	buf[0] = byte(partitionID)
	buf[1] = byte(partitionID >> 8)
	buf[2] = byte(partitionID >> 16)
	buf[3] = byte(partitionID >> 24)

	hashKey := hashing.HashBytes(buf[:4])

	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hashKey
	})

	if idx == len(r.keys) {
		idx = 0
	}

	return r.nodes[r.keys[idx]]
}

// NodeCount returns the number of physical nodes currently in the ring.
func (r *ConsistentHashRing) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodePresent)
}
