package xcsp

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
	"github.com/dmlongo/callidus/domains"
	"github.com/dmlongo/callidus/solver"
)

var hgtools string

func init() {
	path, err := os.Executable()
	if err != nil {
		panic(err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		panic(err)
	}
	hgtools = filepath.Dir(path) + "/libs/hgtools.jar"
}

type HgtConverter struct {
	Doms map[string]string
	Ctrs map[string]Constraint
	Hg   decomp.Hypergraph
	Db   db.Database

	wrkdir  string
	cspName string

	executed bool
}

func NewConverter(wrkdir string, cspName string) domains.Converter {
	return &HgtConverter{
		Doms: make(map[string]string),
		Ctrs: make(map[string]Constraint),
		Hg:   decomp.Hypergraph{},
		Db:   nil,

		wrkdir:  wrkdir,
		cspName: cspName,

		executed: false,
	}
}

func (c *HgtConverter) callHgtools(file string) {
	// TODO add logging
	cmd := exec.Command("java", "-jar", hgtools, "-convert", "-xcsp", "-nofilters", "-print", "-out", c.wrkdir, file)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	hg := decomp.ReadHypergraph(bufio.NewReader(stdout))
	if err := cmd.Wait(); err != nil {
		panic(fmt.Sprintf("hgtools failed: %v: %s", err, stderr.String()))
	}

	domFile := c.wrkdir + c.cspName + ".dom"
	c.Doms = ParseDomains(domFile)
	addDomEdges(&hg, c.Doms)

	ctrFile := c.wrkdir + c.cspName + ".ctr"
	c.Ctrs = ParseConstraints(ctrFile)
	addFilters(&hg, c.Ctrs)

	c.Hg = hg

	data := buildDbString(c.Doms, c.Ctrs)
	c.Db = db.LoadFromString(data)

	c.executed = true
}

func addFilters(hg *decomp.Hypergraph, constraints map[string]Constraint) {
	filters := make([]decomp.Filter, 0)
	z := len(hg.Enc) + 1
	for ctrName, ctr := range constraints {
		if _, ok := hg.Enc[ctrName]; !ok {
			hg.Enc[ctrName] = z
			hg.Dec[z] = ctrName
			var vars []int
			for _, v := range ctr.Variables() {
				if x, ok := hg.Enc[v]; ok {
					vars = append(vars, x)
				} else {
					panic(fmt.Errorf("variable %v from constraint %v doesn't exist", v, ctrName))
				}
			}
			//filters = append(filters, lib.Edge{Name: z, Vertices: vars})
			filters = append(filters, decomp.Filter{Verts: vars})
			z = z + 1
		}
	}
	hg.Filters = filters
}

func addDomEdges(hg *decomp.Hypergraph, doms map[string]string) {
	varEdges := make([]lib.Edge, 0)
	z := len(hg.Enc) + 1
	for eName := range doms {
		if _, ok := hg.Enc[eName]; !ok {
			if !strings.HasPrefix(eName, "Var") {
				panic("")
			}
			hg.Enc[eName] = z
			hg.Dec[z] = eName
			varCode := hg.Enc[eName[3:]]
			varEdges = append(varEdges, lib.Edge{Name: z, Vertices: []int{varCode}})
			z = z + 1
		}
	}
	newEdges := append(hg.Graph.Edges.Slice(), varEdges...)
	hg.Graph = lib.Graph{Edges: lib.NewEdges(newEdges)}
}

func buildDbString(domains map[string]string, constraints map[string]Constraint) string {
	// TODO you can get rid of db entries you don't need
	var b strings.Builder
	for v, dom := range domains {
		fmt.Fprintf(&b, "r,Var%s,%s\n", v, v)
		for _, d := range strings.Split(dom, " ") {
			fmt.Fprintf(&b, "t,%s\n", d)
		}
	}
	for cName, ctr := range constraints {
		if ext, ok := ctr.(*extensionCtr); ok && ext.CType == "supports" { // TODO what to do with conflicts? transform them into filters?
			extVars := strings.ReplaceAll(ext.Vars, " ", ",")
			fmt.Fprintf(&b, "r,%s,%s\n", cName, extVars)
			for _, tup := range strings.Split(ext.Tuples, " ") {
				tup := tup[1 : len(tup)-1]
				fmt.Fprintf(&b, "t,%s\n", tup)
			}
		}
	}
	return b.String()
}

func (c *HgtConverter) ToHypergraph(queryFile string) decomp.Hypergraph {
	if !c.executed {
		c.callHgtools(queryFile)
	}
	return c.Hg
}

func (c *HgtConverter) ToDatabase(dbFile string) db.Database {
	if !c.executed {
		c.callHgtools(dbFile)
	}
	return c.Db
}

func (c *HgtConverter) ToSolution(sol solver.Solution) []int {
	return nil
}
