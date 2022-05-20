package solver

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
)

type Solver interface {
	MakePlan(hd decomp.Hypertree) *Plan
	Solve(pl *Plan, db db.Database) <-chan Solution
}

type Plan struct {
	Cmd  Command
	Subs []*Plan
}

type Command interface {
	Exec()
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
