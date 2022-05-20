package domains

import (
	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
	"github.com/dmlongo/callidus/solver"
)

type Converter interface {
	ToHypergraph(queryFile string) decomp.Hypergraph
	ToDatabase(dbFile string) db.Database
	ToSolution(sol solver.Solution) []int
}
