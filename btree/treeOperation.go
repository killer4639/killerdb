package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		// if node ith key is less than key, then it returns value less than 0 which means found can be updated
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}

		// if node ith key is greater than key, then it returns value greater than 0 which means found can be updated
		if cmp >= 0 {
			break
		}
	}

	return found
}

// Insert a new key value pair in the node
// First we set the header
// After that let's say we want to insert the key at idx
// so we copy from 0 to idx from old node to new node
// then we insert the new key value
// then we copy from idx+1 to new idx
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx)
}

func nodeAppendRange(new BNode, old BNode, newNodeStartIdx uint16, oldNodeStartIdx uint16, len uint16) {
	if len == 0 {
		return
	}
	// Pointers
	for i := uint16(0); i < len; i++ {
		new.setPtr(newNodeStartIdx+i, old.getPtr(oldNodeStartIdx+i))
	}

	// offsets
	newNodeBeginOffset := new.getOffset(newNodeStartIdx)
	oldNodeBeginOffset := old.getOffset(oldNodeStartIdx)

	// It's from 1 to len because whenever we insert a key at index i, then we update the offset of i+1th index, so here we are updating
	// keys from 0 to n-1, so we are updating the offsets from 1 to n
	for i := uint16(1); i <= len; i++ {
		offset := newNodeBeginOffset + (old.getOffset(oldNodeStartIdx+i) - oldNodeBeginOffset)
		new.setOffset(newNodeStartIdx+i, offset)
	}

	// key values

	begin := old.kvPos(oldNodeStartIdx)
	end := old.kvPos(oldNodeStartIdx + len)
	copy(new.data[new.kvPos(newNodeStartIdx):], old.data[begin:end])
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)

	kvPos := new.kvPos(idx)

	keyLen := len(key)
	valLen := len(val)

	// set the offset for the next key
	offset := new.getOffset(idx)
	new.setOffset(idx+1, offset+4+uint16(keyLen)+uint16(valLen))

	// set the sizes of key values
	binary.LittleEndian.PutUint16(new.data[kvPos:], uint16(keyLen))
	binary.LittleEndian.PutUint16(new.data[kvPos+2:], uint16(valLen))

	// set the data
	copy(new.data[(kvPos+4):], key)
	copy(new.data[(kvPos+4+uint16(keyLen)):], val)
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// TODO: understand it more
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// lookup the idx
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, new.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx+1, key, val)
		}

	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)

	default:
		panic("bad node")
	}

	return new

}

func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	kptr := node.getPtr(idx)
	knode := tree.Get(kptr)
	tree.Del(kptr)

	knode = treeInsert(tree, knode, key, val)

	nsplit, splited := nodeSplit3(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	// Get the number of keys in the old node
	nkeys := old.nkeys()
	splitIdx := nkeys / 2 // Split approximately in half

	// Copy first half of keys to left node
	left.setHeader(old.btype(), splitIdx)
	nodeAppendRange(left, old, 0, 0, splitIdx)

	// Copy second half of keys to right node
	right.setHeader(old.btype(), nkeys-splitIdx)
	nodeAppendRange(right, old, 0, splitIdx, nkeys-splitIdx)
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}

	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}

	nodeSplit2(leftleft, middle, left)

	return 3, [3]BNode{leftleft, middle, right}
}

func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.New(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to find the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node!")
	}
}
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.Get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.Del(kptr)
	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.Del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.New(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.Del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.New(merged), merged.getKey(0))
	case mergeDir == 0:
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

// should the updated kid be merged with a sibling?
func shouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := tree.Get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER_SIZE
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.Get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER_SIZE
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{}
}
func (tree *BTree) Delete(key []byte) bool {

	if tree.root == 0 {
		return false
	}
	updated := treeDelete(tree, tree.Get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}
	tree.Del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 { // remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.New(updated)
	}
	return true
}
func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.New(root)
		return
	}
	node := tree.Get(tree.root)
	tree.Del(tree.root)
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.New(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.New(root)
	} else {
		tree.root = tree.New(splitted[0])
	}
}

// nodeReplace2Kid replaces two consecutive child nodes with a single merged node
func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	// Copy nodes before idx
	nodeAppendRange(new, old, 0, 0, idx)
	// Add the merged node
	nodeAppendKV(new, idx, ptr, key, nil)
	// Copy remaining nodes after idx+2 (skipping the two merged nodes)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// Get retrieves the value associated with the given key
func (tree *BTree) GetVal(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}

	node := tree.Get(tree.root)
	for {
		idx := nodeLookupLE(node, key)

		if node.btype() == BNODE_LEAF {
			if idx < node.nkeys() && bytes.Equal(key, node.getKey(idx)) {
				return node.getVal(idx), true
			}
			return nil, false
		}

		node = tree.Get(node.getPtr(idx))
	}
}

func PrintWholeTree(tree *BTree) {
	if tree.root == 0 {
		fmt.Println("Empty tree")
		return
	}
	node := tree.Get(tree.root)

	dfsPrint(tree, node)
}

func dfsPrint(tree *BTree, node BNode) {
	nKeys := node.nkeys()
	for i := uint16(0); i < nKeys; i++ {
		fmt.Print(node.getKey(i))
		fmt.Print()
		fmt.Print(node.getVal(i))
	}

	if node.btype() == BNODE_NODE {
		for i := uint16(0); i < nKeys; i++ {
			dfsPrint(tree, tree.Get(node.getPtr(i)))
		}
	}
}
