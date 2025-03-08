package btree

import (
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func assert(t *testing.T, condition bool) {
	if !condition {
		t.Fatal("assertion failed")
	}
}

func newC(t *testing.T) *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			Get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(t, ok)
				return node
			},
			New: func(node BNode) uint64 {
				assert(t, node.nbytes() <= BTREE_PAGE_SIZE)
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(t, pages[key].data == nil)
				pages[key] = node
				return key
			},
			Del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(t, ok)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) Del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}
