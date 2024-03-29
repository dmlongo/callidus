package db

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

// Tuple represent a row in a relation
type Tuple []string
type Database map[string]*Table

func Load(r *csv.Reader) Database {
	db := make(Database)
	var currName string
	r.FieldsPerRecord = -1
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		kind := record[0]
		switch kind {
		case "r":
			currName = record[1]
			attrs := record[2:]
			db[currName] = NewTable(attrs, true)
		case "t":
			tup := record[1:]
			if _, ok := db[currName].AddTuple(tup); !ok {
				panic(fmt.Errorf("%v is not a valid tuple for %v", tup, currName))
			}
		default:
			panic(fmt.Errorf("%v is not a valid type", kind))
		}
	}

	return db
}

func LoadFromFile(dbPath string) Database {
	csvfile, err := os.Open(dbPath)
	if err != nil {
		panic(fmt.Errorf("can't open %v: %v", dbPath, err))
	}
	return Load(csv.NewReader(csvfile))
}

func LoadFromString(csvfile string) Database {
	return Load(csv.NewReader(strings.NewReader(csvfile)))
}

type Table struct {
	attrs   []string
	AttrPos map[string]int
	Tuples  []Tuple

	Stats *Statistics
}

func NewTable(attrs []string, stats bool) *Table {
	if len(attrs) <= 0 {
		panic(fmt.Errorf("%v is not valid", attrs))
	}

	t := Table{}
	attrPos := make(map[string]int)
	for i, v := range attrs {
		attrPos[v] = i
	}
	t.attrs = attrs
	t.AttrPos = attrPos
	t.Tuples = make([]Tuple, 0)
	if stats {
		t.Stats = NewStatistics(t.attrs)
	}

	return &t
}

func (t *Table) Size() int {
	return len(t.Tuples)
}

func (t *Table) Attributes() []string {
	return t.attrs
}

func (t *Table) Position(attr string) (pos int, ok bool) {
	pos, ok = t.AttrPos[attr]
	return
}

func (t *Table) Empty() bool {
	return len(t.Tuples) == 0
}

func (t *Table) AddTuple(vals []string) (Tuple, bool) {
	if len(t.attrs) != len(vals) {
		return nil, false
	}
	// duplicates allowed
	t.Tuples = append(t.Tuples, vals)
	if t.Stats != nil {
		t.Stats.AddTuple(vals)
	}
	return vals, true
}

func (t *Table) AddTuples(tuples []Tuple) bool {
	for _, tup := range tuples {
		if _, ok := t.AddTuple(tup); !ok {
			return false
		}
	}
	return true
}

func (t *Table) RemoveTuples(idx []int) (bool, error) {
	if len(idx) == 0 {
		return false, nil
	}

	newSize := len(t.Tuples) - len(idx)
	if newSize < 0 {
		return false, fmt.Errorf("new size %v < 0", newSize)
	}
	newTuples := make([]Tuple, 0, newSize)
	if newSize > 0 {
		i := 0
		for _, j := range idx {
			newTuples = append(newTuples, t.Tuples[i:j]...)
			i = j + 1
		}
		newTuples = append(newTuples, t.Tuples[i:]...)
	}
	t.Tuples = newTuples

	// TODO stats update is missing

	return true, nil
}
