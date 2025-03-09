package btree

import "encoding/binary"

// node structure
// | Type (2B) | Num Keys (2B) | Pointers (8B each) | Offsets (2B each) | Key-Value Pairs |
type BNode struct {
	data []byte
}

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

type BTree struct {
	root uint64

	get func(uint64) BNode
	new func(BNode) uint64
	del func(uint64)
}

const HEADER_SIZE = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

// In first two bytes, we have stored the type of node, (node, leaf)
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

// In next two bytes, we have stored the number of keys in the node, this is the metadata
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// This is just metadata,
// btype tells if it is leaf node or not
// next two bytes tell the number of keys
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// Next is stored the pointer to each of the child node, and each ptr is of 8 bytes. so it's position becomes HEADER_SIZE + 8*idx
func (node BNode) getPtr(idx uint16) uint64 {
	pos := HEADER_SIZE + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

// This is setting the same pointer
func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER_SIZE + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// After child pointer, offsets are stored. there are (idx-1) offsets and each offset is of size 2 bytes
// TODO: What is offset?
func offsetPos(node BNode, idx uint16) uint16 {
	return HEADER_SIZE + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	return HEADER_SIZE + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

func (node BNode) getAllKeyVal() map[uint64]string {
	kvs := map[uint64]string{}
	for i := uint16(0); i < node.nkeys(); i++ {
		kvs[binary.LittleEndian.Uint64(node.getKey(i))] = string(node.getVal(i))
	}
	return kvs
}

// Tells you the size of byte array
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
