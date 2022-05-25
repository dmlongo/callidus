package solver

import (
	"hash/fnv"
	"sync"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
)

type joiner struct{}

func NewJoiner() Solver {
	return &joiner{}
}

func (j *joiner) Solve(hg decomp.Hypergraph, hd decomp.Hypertree, data db.Database) <-chan Solution {
	out := make(chan Solution)
	done := make(chan any)

	var empties []<-chan bool
	rootOut := j.buildPipeline(hd, hg.Dec, data, done, &empties)
	empty := merge(done, empties)

	go func() {
		defer close(done)
		defer close(out)

		for {
			select {
			case <-empty:
				out <- make(Solution)
			case s := <-rootOut:
				out <- s.s
			}
		}
	}()

	return out
}

func (j *joiner) buildPipeline(n decomp.Hypertree, dec map[int]string, data db.Database, done <-chan any, empties *[]<-chan bool) <-chan message {
	var cSols []<-chan message
	var indices []hashIndex
	for _, c := range n.Children {
		s := j.buildPipeline(*c, dec, data, done, empties)
		cSols = append(cSols, s)

		conn := lib.Inter(n.Bag, c.Bag)
		var attrs []string
		for _, v := range conn {
			attrs = append(attrs, dec[v])
		}
		idx := hashIndex{
			m:     make(map[uint64][]*prtSol),
			attrs: attrs,
			numCh: len(n.Children),
		}
		indices = append(indices, idx)
	}
	tabs := ToTables(n.Cover, dec, data)
	s, e := join(done, tabs...)
	*empties = append(*empties, e)
	if len(cSols) > 0 {
		sm, em := matcher(done, s, cSols, indices)
		s = sm
		*empties = append(*empties, em)
	}
	return s
}

func matcher(done <-chan any, s <-chan message, cSols []<-chan message, indices []hashIndex) (<-chan message, <-chan bool) {
	out := make(chan message)
	empty := make(chan bool)
	go func() {
		defer close(empty)
		defer close(out)

		sFinished := false
		var buffer []message
		leftovers := make(chan message)
		//cFinished := make([]bool, len(cSols))
		children := mergeCh(done, cSols)
		select {
		case msg := <-s:
			prts := &prtSol{
				tup:     msg.s,
				matches: make([][]Solution, len(cSols)),
			}
			for _, idx := range indices {
				idx.Put(msg.s, prts)
			}
			if msg.trustL && msg.left == 0 {
				sFinished = true
				if len(buffer) > 0 {
					go func() {
						defer func() {
							close(leftovers)
							buffer = nil
						}()

						for _, msg := range buffer {
							select {
							case leftovers <- msg:
							case <-done:
								return
							}
						}
					}()
				} // else if childrenFinished(cFinished) {}
			}
		case msg := <-children:
			k := msg.ch
			if tups := indices[k].Get(msg.s); tups != nil {
				for _, ps := range tups {
					for _, sol := range ps.fire(k, msg.s) {
						select {
						case out <- message{
							ch:     -1,
							trustL: false,
							left:   -1,
							s:      sol,
						}:
						case <-done:
							return
						}
					}
				}
			} else {
				if !sFinished {
					buffer = append(buffer, msg)
				} else {
					// clear incomplete ?
				}
			}
		case msg := <-leftovers:
			k := msg.ch
			if tups := indices[k].Get(msg.s); tups != nil {
				for _, ps := range tups {
					for _, sol := range ps.fire(k, msg.s) {
						select {
						case out <- message{
							ch:     -1,
							trustL: false,
							left:   -1,
							s:      sol,
						}:
						case <-done:
							return
						}
					}
				}
			}
		case <-done:
			return
		}
	}()
	return out, empty
}

func handleSemijoinMsg(msg message) {}

type prtSol struct {
	tup     Solution
	matches [][]Solution
}

// pre: assuming I don't get the same pair (k,s) more than once
func (ps *prtSol) fire(k int, s Solution) []Solution {
	var sols []Solution
	ps.matches[k] = append(ps.matches[k], s)
	var lengths []int
	for i := 0; i < len(ps.matches); i++ {
		if len(ps.matches[i]) == 0 {
			return nil
		}
		lengths = append(lengths, len(ps.matches[i]))
	}
	it := start(lengths, k)
	for it.hasNext() {
		comb := it.next()
		res := ps.tup
		for i, z := range comb {
			res = Extend(res, ps.matches[i][z])
		}
		sols = append(sols, res)
	}
	return sols
}

type iter struct {
	comb      []int
	delivered bool
	lengths   []int
	k         int
}

func start(lengths []int, fix int) *iter {
	comb := make([]int, len(lengths))
	for i := range comb {
		if i != fix {
			comb[i] = 0
		} else {
			comb[i] = lengths[i] - 1
		}
	}
	return &iter{
		comb:      comb,
		delivered: false,
		lengths:   lengths,
		k:         fix,
	}
}

func (it *iter) hasNext() bool {
	if !it.delivered {
		return true
	}
	for i := range it.comb {
		if i != it.k && it.comb[i] != 0 {
			return true
		}
	}
	return false
}

func (it *iter) next() []int {
	if !it.hasNext() {
		return nil
	}
	res := append([]int{}, it.comb...)
	for i := len(it.comb) - 1; i >= 0; i-- {
		if i == it.k {
			continue
		}
		if it.comb[i] == it.lengths[i]-1 {
			it.comb[i] = 0
		} else {
			it.comb[i]++
			break
		}
	}
	it.delivered = true
	return res
}

type hashIndex struct {
	m     map[uint64][]*prtSol
	attrs []string
	numCh int
}

func (hm hashIndex) Put(tup Solution, prts *prtSol) {
	keyVals := proj(hm.attrs, tup)
	key := hashStrings(keyVals...)
	if _, ok := hm.m[key]; !ok {
		hm.m[key] = make([]*prtSol, 0)
	}
	hm.m[key] = append(hm.m[key], prts)
}

func (hm hashIndex) Get(tup Solution) []*prtSol {
	keyVals := proj(hm.attrs, tup)
	key := hashStrings(keyVals...)
	var res []*prtSol
	if tups, ok := hm.m[key]; ok {
		for _, t := range tups {
			if match(tup, t.tup, keyVals) {
				res = append(res, t)
			}
		}
	}
	return res
}

func match(s1, s2 Solution, attrs []string) bool {
	for _, a := range attrs {
		if s1[a] != s2[a] {
			return false
		}
	}
	return true
}

func hashStrings(ss ...string) uint64 {
	var output uint64
	for _, item := range ss {
		h := fnv.New64a()
		h.Write([]byte(item))
		output = output ^ h.Sum64()
	}
	return output
}

func proj(attrs []string, tup Solution) []string {
	var res []string
	for _, a := range attrs {
		attrs = append(attrs, tup[a])
	}
	return res
}

func join(done <-chan any, tabs ...*db.Table) (<-chan message, <-chan bool) {
	out := make(chan message)
	empty := make(chan bool)
	go func() {
		defer close(empty)
		defer close(out)

		var res *db.Table
		switch len(tabs) {
		case 0:
			panic("")
		case 1:
			res = tabs[0]
		case 2:
			res = db.Join(*tabs[0], *tabs[1])
		default:
			// find good join order of tabs
			res = db.Join(*tabs[0], *tabs[1])
			for i := 2; i < len(tabs) && res.Size() > 0; i++ {
				// todo should I write select <-done here?
				res = db.Join(*res, *tabs[i])
			}
		}

		if res.Size() == 0 {
			select {
			case empty <- true:
				return
			case <-done:
				return
			}
		}

		for k, tup := range res.Tuples {
			select {
			case out <- message{
				ch:     -1,
				trustL: true,
				left:   res.Size() - k - 1,
				s:      ToSolution(res.Attributes(), tup),
			}:
			case <-done:
				return
			}
		}
	}()
	return out, empty
}

type message struct {
	ch     int
	trustL bool
	left   int
	s      Solution
}

func mergeCh(done <-chan any, cs []<-chan message) <-chan message {
	var wg sync.WaitGroup
	out := make(chan message)

	wg.Add(len(cs))
	for i, c := range cs {
		go func(i int, c <-chan message) {
			defer wg.Done()
			for b := range c {
				select {
				case out <- message{
					ch:     i,
					trustL: b.trustL,
					left:   b.left,
					s:      b.s,
				}:
				case <-done:
					return
				}
			}
		}(i, c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func merge(done <-chan any, cs []<-chan bool) <-chan bool {
	var wg sync.WaitGroup
	out := make(chan bool)

	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan bool) {
			defer wg.Done()
			for b := range c {
				select {
				case out <- b:
				case <-done:
					return
				}
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
