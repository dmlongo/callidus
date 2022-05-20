package decomp

import (
	"fmt"

	"github.com/dmlongo/callidus/db"
)

type Evaluator interface {
	Eval(tree Hypertree) int
	//EvalNode(node Hypertree) int
}

type NumNodes struct {
}

func (ev *NumNodes) Eval(tree Hypertree) int {
	return tree.Size() * -1
}

func (ev *NumNodes) EvalNode(node Hypertree) int {
	return 1
}

type TrivialGrndEval struct {
	Doms map[int]int
}

func (ev *TrivialGrndEval) Eval(tree Hypertree) int {
	sum := 0
	for _, n := range tree.dfsPre() {
		sum += ev.EvalNode(*n)
	}
	return sum
}

func (ev *TrivialGrndEval) EvalNode(node Hypertree) int {
	m := 1
	for _, v := range node.Bag {
		m *= ev.Doms[v]
	}
	return m
}

type naiveGrndEval struct {
	doms db.Database
	dec  map[int]string
}

func NewNaiveGrndEval(data db.Database, hg Hypergraph) Evaluator {
	doms := make(db.Database)
	for _, table := range data {
		for _, attr := range table.Attributes() {
			if domTable, ok := doms[attr]; ok {
				domTable.AddTuples(db.Project(table, attr).Tuples)
				doms[attr] = db.Distinct(domTable, attr)
			} else {
				doms[attr] = db.Distinct(table, attr)
			}
		}
	}
	return &naiveGrndEval{doms: doms, dec: hg.Dec}
}

func (ev *naiveGrndEval) Eval(tree Hypertree) int {
	cost := 0
	for _, n := range tree.dfsPre() {
		m := 1
		for _, v := range n.Bag {
			if attr, ok := ev.dec[v]; ok {
				m *= ev.doms[attr].Size()
			} else {
				panic(fmt.Errorf("entry for %v doesn't exist", v))
			}
		}
		cost += m
	}
	return cost
}

type onePassUpEval struct {
	StatsDB    StatisticsDB
	toRestore  StatisticsDB
	toRestKeys [][]int
}

func NewOnePassUpEval(data db.Database, hg Hypergraph) Evaluator {
	return &onePassUpEval{
		StatsDB:    StatsFromDB(data, hg.Enc),
		toRestore:  make(StatisticsDB),
		toRestKeys: nil,
	}
}

func (ev *onePassUpEval) Eval(tree Hypertree) int {
	cost := 0
	for _, n := range tree.dfsPost() {
		cost += ev.evalNode(n)
		ev.save(n.IntCover())
		/*for _, child := range n.children {
			cost += ev.evalEdge(n, child)
		}*/
	}
	ev.restore()
	return cost
}

func (ev *onePassUpEval) evalNode(n *Hypertree) int {
	if _, ok := ev.StatsDB.Stats(n.IntCover()); !ok {
		var jTables []*db.Statistics
		for _, e := range n.Cover.Slice() {
			if eStats, ok := ev.StatsDB.Stats([]int{e.Name}); !ok {
				panic(fmt.Errorf("no stats for single edge %v", e.Name))
			} else {
				jTables = append(jTables, eStats)
			}
		}
		_, stats := db.EstimateJoinSize(jTables)

		ev.StatsDB.Put(n.IntCover(), stats)
	}

	stats, _ := ev.StatsDB.Stats(n.IntCover())
	return stats.Size
}

func (ev *onePassUpEval) evalEdge(par *Hypertree, child *Hypertree) int {
	parStats, parOk := ev.StatsDB.Stats(par.IntCover())
	childStats, childOk := ev.StatsDB.Stats(child.IntCover())

	if !parOk || !childOk {
		panic(fmt.Errorf("no stats for edge (%v,%v)", par.Cover, child.Cover))
	}

	newParSize, newParStats := db.EstimateSemijoinSize(parStats, childStats)
	ev.StatsDB.Put(par.IntCover(), newParStats)
	return newParSize
}

func (ev *onePassUpEval) save(entry []int) {
	if oldValue, ok := ev.StatsDB.Stats(entry); ok {
		ev.toRestore.Put(entry, oldValue)
		ev.toRestKeys = append(ev.toRestKeys, entry)
	} else {
		panic(fmt.Errorf("table %v not present", entry))
	}
}

func (ev *onePassUpEval) restore() {
	for _, k := range ev.toRestKeys {
		if oldValue, ok := ev.toRestore.Stats(k); ok {
			ev.StatsDB.Put(k, oldValue)
		} else {
			panic(fmt.Errorf("table %v not present", k))
		}
	}
	ev.toRestore = make(StatisticsDB)
	ev.toRestKeys = nil
}

type DBEvaluator struct {
	StatsDB    StatisticsDB
	toRestore  StatisticsDB
	toRestKeys [][]int
}

func NewEvaluator(data db.Database, hg Hypergraph) DBEvaluator {
	return DBEvaluator{
		StatsDB:    StatsFromDB(data, hg.Enc),
		toRestore:  make(StatisticsDB),
		toRestKeys: nil,
	}
}

func (ev *DBEvaluator) Eval(node *Hypertree) int {
	cost := 0
	var n *Hypertree
	dfs := node.dfsPre()
	for len(dfs) > 0 {
		n, dfs = dfs[len(dfs)-1], dfs[:len(dfs)-1]
		ev.EvalNode(n)
		ev.save(n.IntCover())
		cost += ev.EvalNodeWithFilters(n)
		for _, child := range n.children {
			cost += ev.EvalEdge(n, child)
		}
	}
	ev.restore()
	return cost
}

func (ev *DBEvaluator) EvalNode(n *Hypertree) int {
	if _, ok := ev.StatsDB.Stats(n.IntCover()); !ok {
		var jTables []*db.Statistics
		for _, e := range n.Cover.Slice() {
			if eStats, ok := ev.StatsDB.Stats([]int{e.Name}); !ok {
				panic(fmt.Errorf("no stats for single edge %v", e.Name))
			} else {
				jTables = append(jTables, eStats)
			}
		}
		_, stats := db.EstimateJoinSize(jTables)

		ev.StatsDB.Put(n.IntCover(), stats)
	}

	stats, _ := ev.StatsDB.Stats(n.IntCover())
	return stats.Size
}

func (ev *DBEvaluator) EvalNodeWithFilters(n *Hypertree) int {
	if _, ok := ev.StatsDB.Stats(n.IntCover()); !ok {
		var jTables []*db.Statistics
		for _, e := range n.Cover.Slice() {
			if eStats, ok := ev.StatsDB.Stats([]int{e.Name}); !ok {
				panic(fmt.Errorf("no stats for single edge %v", e.Name))
			} else {
				jTables = append(jTables, eStats)
			}
		}
		_, stats := db.EstimateJoinSize(jTables)

		ev.StatsDB.Put(n.IntCover(), stats)
	}

	stats, _ := ev.StatsDB.Stats(n.IntCover())
	for _, f := range n.Filters {
		_, stats = db.EstimateSelectionSize(stats, f.cond)
	}
	return stats.Size
}

func (ev *DBEvaluator) EvalEdge(par *Hypertree, child *Hypertree) int {
	parStats, parOk := ev.StatsDB.Stats(par.IntCover())
	childStats, childOk := ev.StatsDB.Stats(child.IntCover())

	if !parOk || !childOk {
		panic(fmt.Errorf("no stats for edge (%v,%v)", par.Cover, child.Cover))
	}

	newParSize, newParStats := db.EstimateSemijoinSize(parStats, childStats)
	ev.StatsDB.Put(par.IntCover(), newParStats)
	return newParSize

	/*parTables := qe.toTables(par.sep)
	childTables := qe.toTables(child.sep)
	if len(par.sep.Slice()) == 1 && len(child.sep.Slice()) == 1 {
		return db.HgramSemijoinSize(parTables[0], childTables[0])
	} // non funziona, non tengo conto che par puo' avere altri figli*/

	// size_{child} = sel_{child} * par.size
	// sel_{child} = (expected cardinality of q_{child}) / (prod of q_{child} tables)
	/*num := child.size * par.size
	den := 1
	childTables := qe.toTables(child.sep)
	for _, t := range childTables {
		den *= t.Size()
	}
	return int(math.Round(float64(num) / float64(den)))
	// I think there are smarter ways to do this
	*/
}

func (ev *DBEvaluator) save(entry []int) {
	if oldValue, ok := ev.StatsDB.Stats(entry); ok {
		ev.toRestore.Put(entry, oldValue)
		ev.toRestKeys = append(ev.toRestKeys, entry)
	} else {
		panic(fmt.Errorf("table %v not present", entry))
	}
}

func (ev *DBEvaluator) restore() {
	for _, k := range ev.toRestKeys {
		if oldValue, ok := ev.toRestore.Stats(k); ok {
			ev.StatsDB.Put(k, oldValue)
		} else {
			panic(fmt.Errorf("table %v not present", k))
		}
	}
	ev.toRestore = make(StatisticsDB)
	ev.toRestKeys = nil
}
