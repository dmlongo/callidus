package solver

import (
	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
)

type joiner struct {
}

func NewSolver() Solver {
	return &joiner{}
}

func (j *joiner) MakePlan(hd decomp.Hypertree) *Plan {
	return nil
}

func (j *joiner) Solve(pl *Plan, db db.Database) <-chan Solution {
	return nil
}
