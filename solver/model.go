package solver

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
)

type Solver interface {
	Solve(hg decomp.Hypergraph, hd decomp.Hypertree, data db.Database) <-chan Solution
}

type Plan struct {
	Cmd  Command
	Subs []*Plan
}

type Command interface {
	Exec(done <-chan any) <-chan Solution
}

// Solution of a problem instance
type Solution map[string]string

func (sol Solution) SortVars() []string {
	vars := make([]string, 0, len(sol))
	for v := range sol {
		vars = append(vars, v)
	}
	sort.Strings(vars)
	return vars
}

// Print a solution to stdout
func (sol Solution) Print() {
	vars := sol.SortVars()
	var sb strings.Builder
	for _, v := range vars {
		sb.WriteString(v)
		sb.WriteByte(' ')
	}
	sb.WriteString("-> ")
	for _, v := range vars {
		sb.WriteString(sol[v]) //strconv.Itoa(sol[v]))
		sb.WriteByte(' ')
	}
	fmt.Println(sb.String())
}

func (sol Solution) Equals(oth Solution) bool {
	if len(sol) != len(oth) {
		return false
	}
	for k, v := range sol {
		if vOth, ok := oth[k]; !ok || vOth != v {
			return false
		}
	}
	return true
}

func Extend(s1, s2 Solution) Solution {
	res := make(Solution)
	for k1, v1 := range s1 {
		if v2, ok := s2[k1]; ok {
			if v1 != v2 {
				panic(fmt.Errorf("no match for %v, %v", s1, s2))
			}
		}
		res[k1] = v1
	}
	for k2, v2 := range s2 {
		res[k2] = v2
	}
	return res
}

// pre: len(attrs) == len(tup)
func ToSolution(attrs []string, tup db.Tuple) Solution {
	s := make(Solution)
	for i, a := range attrs {
		s[a] = tup[i]
	}
	return s
}

func ToTables(edges lib.Edges, dec map[int]string, data db.Database) []*db.Table {
	var tabs []*db.Table
	for _, e := range edges.Slice() {
		eName := dec[e.Name]
		if t, ok := data[eName]; ok {
			tabs = append(tabs, t)
		} else {
			panic(fmt.Errorf("table %v not in the database", eName))
		}
	}
	return tabs
}

/*
func (sol Solution) WriteToFile(out string) {
	err := os.RemoveAll(out)
	if err != nil {
		panic(err)
	}
	file, err := os.OpenFile(out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		panic(err)
	}
	for indexResult, res := range sol {
		_, err = file.WriteString("Sol " + strconv.Itoa(indexResult+1) + "\n")
		if err != nil {
			panic(err)
		}
		for key, value := range res {
			_, err = file.WriteString(key + " -> " + strconv.Itoa(value) + "\n")
			if err != nil {
				panic(err)
			}
		}
	}
	_, err = file.WriteString("Solutions found: " + strconv.Itoa(len(sol)))
	if err != nil {
		panic(err)
	}
}
*/
