package solver

import (
	"fmt"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
)

type joiner struct {
	dec  map[int]string
	data db.Database
}

func NewSolver(hg decomp.Hypergraph, data db.Database) Solver {
	return &joiner{
		dec:  hg.Dec,
		data: data,
	}
}

func (j *joiner) MakePlan(hd decomp.Hypertree) *Plan {
	var p *Plan
	myTables := j.getTables(hd.Cover)
	switch len(myTables) {
	case 0:
		panic("")
	case 1:
		p = &Plan{Cmd: &EnumCmd{table: myTables[0]}}
		for _, c := range hd.Children {
			p.Subs = append(p.Subs, j.MakePlan(*c))
		}
	case 2:
		t := db.Join(*myTables[0], *myTables[1])
		p = &Plan{Cmd: &EnumCmd{table: t}}
		for _, c := range hd.Children {
			p.Subs = append(p.Subs, j.MakePlan(*c))
		}
	default: // >= 3
		res := db.Join(*myTables[0], *myTables[1])
		for i := 2; i < len(myTables); i++ {
			res = db.Join(*res, *myTables[i])
		}
		p = &Plan{Cmd: &EnumCmd{table: res}}
		for _, c := range hd.Children {
			p.Subs = append(p.Subs, j.MakePlan(*c))
		}
	}
	return p
}

func (j *joiner) getTables(edges lib.Edges) []*db.Table {
	var tabs []*db.Table
	for _, e := range edges.Slice() {
		eName := j.dec[e.Name]
		if t, ok := j.data[eName]; ok {
			tabs = append(tabs, t)
		} else {
			panic(fmt.Errorf("table %v not in the database", eName))
		}
	}
	return tabs
}

func (j *joiner) Solve(pl *Plan, db db.Database) <-chan Solution {
	return nil
}

type EnumCmd struct {
	table *db.Table
}

func (cmd *EnumCmd) Exec(done <-chan any) <-chan Solution {
	out := make(chan Solution)
	go func() {
		defer close(out)

		attrs := cmd.table.Attributes()
		for _, t := range cmd.table.Tuples {
			select {
			case out <- ToSolution(attrs, t):
			case <-done:
				return
			}
		}
	}()
	return out
}
