package skiplist

import (
	"bytes"
	"math/rand"
)

const maxLevel = 20
const p = 0.25

type Value struct {
	Flag    byte
	Version uint64
	Data    []byte
}

type Element struct {
	Key   []byte
	Value []byte
}

func keyLess(lhs Element, rhs Element) bool {
	return bytes.Compare(lhs.Key, rhs.Key) < 0
}

func keyEqual(lhs Element, rhs Element) bool {
	return bytes.Equal(lhs.Key, rhs.Key)
}

type node struct {
	element Element
	level   int8
	forward [maxLevel]*node
}

func (n *node) next(level int8) *node {
	return n.forward[level]
}

type SkipList struct {
	head  *node
	level int8
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  new(node),
		level: 0,
	}
}

func (sl *SkipList) Insert(e Element) {
	update := make([]*node, maxLevel)
	cur := sl.head
	for i := sl.level; i >= 0; i-- {
		for ; cur.next(i) != nil && keyLess(cur.next(i).element, e); cur = cur.next(i) {

		}
		update[i] = cur
	}
	cur = cur.next(0)
	if cur != nil && keyEqual(cur.element, e) {
		cur.element = e
		return
	}
	newLevel := sl.getRandomLevel()
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}
	newNode := &node{
		element: e,
		level:   newLevel,
	}
	for i := int8(0); i <= newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	e := Element{Key: key}
	cur := sl.head
	for i := sl.level; i >= 0; i-- {
		for ; cur.next(i) != nil && keyLess(cur.next(i).element, e); cur = cur.next(i) {

		}
	}
	cur = cur.next(0)
	if cur == nil || !keyEqual(cur.element, e) {
		return nil, false
	}
	return cur.element.Value, true
}

func (sl *SkipList) Begin() *Iterator {
	return &Iterator{node: sl.head.forward[0]}
}

func (sl *SkipList) getRandomLevel() int8 {
	level := 0
	for level < maxLevel && rand.Float32() < p {
		level++
	}
	return int8(level)
}

func (sl *SkipList) size() int {
	count := 0
	for iter := sl.Begin(); iter.Valid(); iter.Next() {
		count++
	}
	return count
}

type Iterator struct {
	node *node
}

func NewIterator(list *SkipList) *Iterator {
	return &Iterator{
		node: list.head,
	}
}

func (i *Iterator) Valid() bool {
	return i.node != nil
}

func (i *Iterator) Next() {
	i.node = i.node.next(0)
}

func (i *Iterator) Key() []byte {
	return i.node.element.Key
}

func (i *Iterator) Value() []byte {
	return i.node.element.Value
}
