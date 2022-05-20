package datalog

import (
	"os"
	"strings"

	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
	"github.com/dmlongo/callidus/domains"
	"github.com/dmlongo/callidus/solver"
)

type DgConverter struct {
	Db db.Database
}

func NewConverter(wrkdir string, ruleName string, factsName string) domains.Converter {
	return &DgConverter{
		Db: nil,
	}
}

func (c *DgConverter) ToHypergraph(queryFile string) decomp.Hypergraph {
	// assuming the head of the rule is in the first line
	// no filters

	dat, err := os.ReadFile(queryFile)
	if err != nil {
		panic(err)
	}
	rule := string(dat)
	k := strings.Index(rule, "\n")
	head := rule[:k]
	body := rule[k+1:]
	return decomp.MakeHypergraph(body, "", head)
}

func (c *DgConverter) ToDatabase(dbFile string) db.Database {
	return db.LoadFromFile(dbFile)
}

func (c *DgConverter) ToSolution(sol solver.Solution) []int {
	return nil
}
