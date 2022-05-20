package db

import (
	"container/heap"
	"fmt"
)

type Condition func(t Tuple) bool

func Semijoin(l *Table, r Table) (*Table, bool) {
	joinIdx := commonAttrs(*l, r)
	if len(joinIdx) == 0 {
		return l, false
	}

	var tupToDel []int
	for i, leftTup := range l.Tuples {
		delete := true
		for _, rightTup := range r.Tuples {
			if match(leftTup, rightTup, joinIdx) {
				delete = false
				break
			}
		}
		if delete {
			tupToDel = append(tupToDel, i)
		}
	}

	res, err := l.RemoveTuples(tupToDel)
	if err != nil {
		panic(err)
	}
	return l, res
}

func Join(l Table, r Table) *Table {
	if l.Size() < r.Size() {
		l, r = r, l
	}
	joinIdx := commonAttrs(l, r)
	newAttrs, _ := JoinAttrs(&l, &r)
	newTab := NewTable(newAttrs, false) // todo compute stats?
	for _, lTup := range l.Tuples {
		for _, rTup := range r.Tuples {
			if match(lTup, rTup, joinIdx) {
				newTup := joinedTuple(newAttrs, lTup, rTup, r.AttrPos)
				newTab.AddTuple(newTup)
			}
		}
	}
	return newTab
}

func Select(r *Table, c Condition) (*Table, bool) {
	var tupToDel []int
	for i, tup := range r.Tuples {
		if !c(tup) {
			tupToDel = append(tupToDel, i)
		}
	}
	res, err := r.RemoveTuples(tupToDel)
	if err != nil {
		panic(err)
	}
	return r, res
}

func commonAttrs(left Table, right Table) [][]int {
	var out [][]int
	rev := len(right.attrs) < len(left.attrs)
	if rev {
		left, right = right, left
	}
	for iLeft, varLeft := range left.attrs {
		if iRight, found := right.AttrPos[varLeft]; found {
			if rev {
				out = append(out, []int{iRight, iLeft})
			} else {
				out = append(out, []int{iLeft, iRight})
			}
		}
	}
	return out
}

func match(left Tuple, right Tuple, joinIndex [][]int) bool {
	for _, z := range joinIndex {
		if left[z[0]] != right[z[1]] {
			return false
		}
	}
	return true
}

func joinedTuple(attrs []string, lTup Tuple, rTup Tuple, rAttrPos map[string]int) Tuple {
	res := make(Tuple, 0, len(attrs))
	res = append(res, lTup...)
	for _, v := range attrs[len(lTup):] {
		i := rAttrPos[v]
		res = append(res, rTup[i])
	}
	return res
}

func Project(r *Table, attr string) *Table {
	if j, ok := r.AttrPos[attr]; ok {
		res := NewTable([]string{attr}, true)
		for _, tup := range r.Tuples {
			res.AddTuple(Tuple{tup[j]})
		}
		return res
	} else {
		return nil
	}
}

func Distinct(r *Table, attrs ...string) *Table {
	if len(attrs) == 0 {
		attrs = r.Attributes()
	}

	var pr []int
	for _, a := range attrs {
		if i, ok := r.Position(a); ok {
			pr = append(pr, i)
		} else {
			panic(fmt.Errorf("attribute %v doesn't exist", a))
		}
	}

	var h tupleHeap
	heap.Init(&h)
	for _, tup := range r.Tuples {
		heap.Push(&h, projT(tup, pr))
	}

	var last Tuple
	res := NewTable(attrs, true)
	for h.Len() > 0 {
		curr := heap.Pop(&h).(Tuple)
		if last == nil || !equal(last, curr) {
			res.AddTuple(curr)
			last = curr
		}
	}

	return res
}

func equal(t1, t2 Tuple) bool {
	if len(t1) != len(t2) {
		return false
	}
	for k := 0; k < len(t1); k++ {
		if t1[k] != t2[k] {
			return false
		}
	}
	return true
}

func projT(tup Tuple, attrs []int) Tuple {
	var res Tuple
	for _, j := range attrs {
		res = append(res, tup[j])
	}
	return res
}

type tupleHeap []Tuple

func (h tupleHeap) Len() int      { return len(h) }
func (h tupleHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h tupleHeap) Less(i, j int) bool {
	t1, t2 := h[i], h[j]
	if len(t1) != len(t2) {
		return len(t1) < len(t2) // this case shouldn't happen though
	}
	for k := 0; k < len(t1); k++ {
		if t1[k] != t2[k] {
			return t1[k] < t2[k]
		}
	}
	return true
}

func (h *tupleHeap) Push(x any) {
	*h = append(*h, x.(Tuple))
}

func (h *tupleHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
