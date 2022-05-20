package decomp

import (
	"github.com/cem-okulmus/BalancedGo/lib"
)

type Hypertree struct {
	Bag     []int
	Cover   lib.Edges
	Filters []Filter

	parent   *Hypertree
	children []*Hypertree
	fEdges   [][]Filter

	//root *Hypertree
}

func NewHypertree(bag []int, cover lib.Edges, filts []Filter) *Hypertree {
	return &Hypertree{
		Bag:     bag,
		Cover:   cover,
		Filters: filts,

		parent:   nil,
		children: nil,
		fEdges:   nil,
	}
}

func (tree *Hypertree) IntCover() []int {
	var cov []int
	for _, e := range tree.Cover.Slice() {
		cov = append(cov, e.Name)
	}
	return cov
}

func (tree *Hypertree) dfsPre() []*Hypertree {
	var res []*Hypertree
	var n *Hypertree
	open := []*Hypertree{tree}
	for len(open) > 0 {
		n, open = open[len(open)-1], open[:len(open)-1]
		res = append(res, n)
		for i := range n.children {
			open = append(open, n.children[len(n.children)-i-1])
		}
	}
	return res
}

func (tree *Hypertree) dfsPost() []*Hypertree {
	var res []*Hypertree
	var n *Hypertree
	open := []*Hypertree{tree}
	for len(open) > 0 {
		n, open = open[len(open)-1], open[:len(open)-1]
		res = append(res, n)
		for i := range n.children {
			open = append(open, n.children[i])
		}
	}
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

func (tree *Hypertree) Root() *Hypertree {
	/*if tree.root != nil {
		return tree.root // todo remove - cannot see global changes
	}*/

	root := tree
	for root.parent != nil {
		root = root.parent
	}
	//tree.root = root
	return root
}

func (tree *Hypertree) Size() int {
	return len(tree.Root().dfsPre())
}

func (tree *Hypertree) Successors() <-chan *Hypertree {
	out := make(chan *Hypertree)
	go func() {
		defer close(out)

		// todo no filters into account till now
		for _, n := range tree.dfsPre() {
			if n.Cover.Len() > 1 {
				pConn := []int{}
				if n.parent != nil {
					pConn = lib.Inter(n.parent.Bag, n.Bag)
				}
				for _, e := range n.Cover.Slice() {
					lambdaMin := setDiff(n.Cover, e)
					/*if n == n.Root() && !lib.Subset(outVerts, lambdaMin.Vertices()) {
						break
					}*/
					if !lib.Subset(pConn, lambdaMin.Vertices()) { // necessary condition
						break
					}

					canMakeNew := true
					var pushes []*Hypertree
					for j, s := range n.children {
						conn := lib.Inter(n.Bag, s.Bag)
						if !lib.Subset(conn, lambdaMin.Vertices()) {
							canMakeNew = false
							pushes = nil

							// s votes a veto against moving e
							// s is now the only one who can get e
							canPushS := true
							for k := j + 1; k < len(n.children); k++ { // children before j do not oppose
								c := n.children[k]
								conn := lib.Inter(n.Bag, c.Bag)
								if !lib.Subset(conn, lambdaMin.Vertices()) {
									canPushS = false
									break
								}
							}
							if canPushS {
								pushes = append(pushes, s)
							}

							break
						}
						pushes = append(pushes, s)
					}

					if canMakeNew {
						out <- makeNewNode(n, lambdaMin, e)
					}
					for _, m := range pushes {
						out <- pushInto(n, m, lambdaMin, e)
					}
				}
			}
		}
	}()
	return out
}

func pushInto(n *Hypertree, m *Hypertree, lambdaMin lib.Edges, e lib.Edge) *Hypertree {
	nn := NewHypertree(lambdaMin.Vertices(), lambdaMin, nil) // todo filters
	for _, c := range n.children {
		cc := copySubtree(c)
		nn.children = append(nn.children, cc)
		cc.parent = nn
	}

	lambda := lib.NewEdges(append(m.Cover.Slice(), e))
	mm := NewHypertree(lambda.Vertices(), lambda, nil) // todo filters
	nn.children = append(nn.children, mm)
	mm.parent = nn

	pp := copyDiffTree(n)
	if pp != nil {
		pp.children = append(pp.children, nn)
		nn.parent = pp
	}
	return nn.Root()
}

func makeNewNode(n *Hypertree, lambdaMin lib.Edges, e lib.Edge) *Hypertree {
	nn := NewHypertree(lambdaMin.Vertices(), lambdaMin, nil) // todo filters
	for _, c := range n.children {
		cc := copySubtree(c)
		nn.children = append(nn.children, cc)
		cc.parent = nn
	}

	lambda := lib.NewEdges([]lib.Edge{e})
	m := NewHypertree(lambda.Vertices(), lambda, nil) // todo filters
	nn.children = append(nn.children, m)
	m.parent = nn

	pp := copyDiffTree(n)
	if pp != nil {
		pp.children = append(pp.children, nn)
		nn.parent = pp
	}
	return nn.Root()
}

func copyDiffTree(n *Hypertree) *Hypertree {
	open := []*Hypertree{n.Root()}
	parents := []*Hypertree{nil}
	var curr, m, p, res *Hypertree
	for len(open) > 0 {
		m, open = open[0], open[1:]
		p, parents = parents[0], parents[1:]

		if m == n {
			res = p
			continue
		}

		curr = NewHypertree(m.Bag, m.Cover, m.Filters)
		curr.parent = p

		for _, c := range m.children {
			open = append(open, c)
			parents = append(parents, curr)
		}
	}
	return res
}

func copySubtree(n *Hypertree) *Hypertree {
	open := []*Hypertree{n}
	parents := []*Hypertree{nil}
	var curr, m, p *Hypertree
	for len(open) > 0 {
		m, open = open[0], open[1:]
		p, parents = parents[0], parents[1:]

		curr = NewHypertree(m.Bag, m.Cover, m.Filters)
		curr.parent = p

		for _, c := range m.children {
			open = append(open, c)
			parents = append(parents, curr)
		}
	}
	return curr.Root()
}

func setDiff(set lib.Edges, f lib.Edge) lib.Edges {
	var res []lib.Edge
	for _, e := range set.Slice() {
		if e.Name != f.Name {
			res = append(res, e)
		}
	}
	return lib.NewEdges(res)
}

func DecompH(hg Hypergraph, ev Evaluator) *Hypertree {
	currHT := NewHypertree(hg.Graph.Vertices(), hg.Graph.Edges, hg.Filters)
	bestHT, bestCost := currHT, ev.Eval(*currHT)

	for {
		flag := false
		for succHT := range currHT.Successors() {
			if !lib.Subset(hg.Out, succHT.Bag) { // todo quick fix - integrate into Successors()
				continue
			}
			if succCost := ev.Eval(*succHT); succCost < bestCost {
				bestHT = succHT
				bestCost = succCost
				flag = true
			}
		}
		if !flag {
			break
		} else {
			currHT = bestHT
		}
	}

	return bestHT
}
