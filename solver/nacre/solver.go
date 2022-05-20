package nacre

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
	"github.com/dmlongo/callidus/domains/xcsp"
	"github.com/dmlongo/callidus/files"
	"github.com/dmlongo/callidus/solver"
)

type nacreSolver struct {
	ctrs map[string]xcsp.Constraint
	doms map[string]string
	dec  map[int]string

	wrkdir string
}

func NewSolver(wrkdir string, ctrs map[string]xcsp.Constraint, doms map[string]string, dec map[int]string) solver.Solver {
	return &nacreSolver{
		ctrs: ctrs,
		doms: doms,
		dec:  dec,

		wrkdir: wrkdir,
	}
}

func (s *nacreSolver) MakePlan(hd decomp.Hypertree) *solver.Plan {
	subCspFolder := files.MakeDir(s.wrkdir + "subs/")

	var cmds []*solver.Plan

	var toVisit []*decomp.Hypertree
	if !reflect.DeepEqual(hd, decomp.Hypertree{}) {
		toVisit = append(toVisit, hd.Root())
	}
	var currNode *decomp.Hypertree
	for i := 0; len(toVisit) > 0; i++ {
		currNode, toVisit = toVisit[len(toVisit)-1], toVisit[:len(toVisit)-1]

		nodeCtrs, nodeVars := s.filterCtrsVars(currNode)
		subFile := subCspFolder + "sub" + strconv.Itoa(i) + ".xml"
		xcsp.CreateXCSPInstance(nodeCtrs, nodeVars, subFile)

		bag := make([]string, len(currNode.Bag))
		for k, v := range currNode.Bag {
			bag[k] = s.dec[v] //strconv.Itoa(v)
		}
		cmd := &solver.Plan{
			Cmd: &CallNacreCmd{
				cspFile: subFile,
				vars:    bag,
			},
		}
		if len(cmds) > 0 {
			par := cmds[len(cmds)-1]
			par.Subs = append(par.Subs, cmd)
		}
		cmds = append(cmds, cmd)
	}

	var res *solver.Plan
	if len(cmds) > 0 {
		res = cmds[0]
	}
	return res
}

func (s *nacreSolver) filterCtrsVars(n *decomp.Hypertree) ([]xcsp.Constraint, map[string]string) {
	outCtrs := make([]xcsp.Constraint, 0, n.Cover.Len())
	outVars := make(map[string]string)
	for _, e := range n.Cover.Slice() {
		eName := s.dec[e.Name]
		if !strings.HasPrefix(eName, "Var") {
			c := s.ctrs[eName]
			outCtrs = append(outCtrs, c)
			for _, v := range c.Variables() {
				if _, ok := outVars[v]; !ok {
					outVars[v] = s.doms[v]
				}
			}
		} else {
			// todo what if the variable isn't in any constraint?
			// add to domain
		}
	}
	return outCtrs, outVars
}

func (s *nacreSolver) Solve(pl *solver.Plan, db db.Database) <-chan solver.Solution {
	out := make(chan solver.Solution)
	go func() {
		defer close(out)

		cmd := pl.Cmd.(*CallNacreCmd)
		cmd.Exec()

		if cmd.sat {
			for _, t := range cmd.table.Tuples {
				s := make(solver.Solution)
				for p, k := range cmd.table.Attributes() {
					s[k] = t[p]
				}
				out <- s
			}
		}
	}()
	return out
}

var nacre string

func init() {
	path, err := os.Executable()
	if err != nil {
		panic(err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		panic(err)
	}
	nacre = filepath.Dir(path) + "/libs/nacre_mini_release"
}

type CallNacreCmd struct {
	cspFile string
	vars    []string
	sat     bool
	table   *db.Table
}

func (n *CallNacreCmd) Exec() {
	cmd := exec.Command(nacre, n.cspFile, "-complete", "-sols", "-printSols")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	if err := cmd.Start(); err != nil {
		panic(err)
	}

	sat, tuples := readTuples(bufio.NewReader(stdout), n.cspFile, n.vars)
	if err := cmd.Wait(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() != 40 {
			panic(fmt.Sprintf("nacre failed: %v:", err))
		}
	}
	n.sat = sat
	n.table = tuples
}

func readTuples(reader *bufio.Reader, cspFile string, attrs []string) (bool, *db.Table) {
	solFound := false
	table := db.NewTable(attrs, true)
	for {
		line, eof := files.ReadLine(reader)
		if eof {
			break
		}
		if strings.HasPrefix(line, "v") {
			tup := makeTuple(line, cspFile, table.AttrPos)
			if _, added := table.AddTuple(tup); !added {
				panic(fmt.Sprintf("%s: Could not add tuple %v", cspFile, tup))
			}
			solFound = true
		}
	}
	return solFound, table
}

var listRegex = regexp.MustCompile(`.*<list>(.*)</list>.*`)
var valuesRegex = regexp.MustCompile(`.*<values>(.*)</values>.*`)

func makeTuple(line string, cspFile string, attrs map[string]int) db.Tuple {
	matchesVal := valuesRegex.FindStringSubmatch(line)
	if len(matchesVal) < 2 {
		panic(cspFile + ", bad values= " + line)
	}
	matchesList := listRegex.FindStringSubmatch(line)
	if len(matchesList) < 2 {
		panic(cspFile + ", bad list= " + line)
	}

	list := strings.Split(strings.TrimSpace(matchesList[1]), " ")
	tup := make([]string, len(attrs))
	z := 0
	for i, value := range strings.Split(strings.TrimSpace(matchesVal[1]), " ") {
		if p, ok := attrs[list[i]]; ok {
			tup[p] = value
			z++
		}
	}
	if z != len(attrs) {
		panic(fmt.Sprintf("Did not find enough variables %v/%v, list: %v", z, len(attrs), list))
	}
	return tup
}
