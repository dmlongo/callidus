package decomp

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/dmlongo/callidus/db"
)

// SizeEstimates associates statistics to combinations of edges
type StatisticsDB map[uint64]*db.Statistics

func StatsFromDB(data db.Database, encoding map[string]int) StatisticsDB {
	res := make(StatisticsDB)
	for tName, tab := range data {
		eName := encoding[tName]
		res.Put([]int{eName}, tab.Stats)
	}
	return res
}

// Put the statistics of an edge combination into the map
func (sdb StatisticsDB) Put(edges []int, stats *db.Statistics) {
	h := hashInts(edges)
	sdb[h] = stats
}

// Statistics of an edge combination
func (sdb StatisticsDB) Stats(edges []int) (*db.Statistics, bool) {
	h := hashInts(edges)
	if c, ok := sdb[h]; ok {
		return c, true
	}
	return nil, false
}

func hashInts(edges []int) uint64 {
	var output uint64
	for _, item := range edges {
		h := fnv.New64a()
		bs := make([]byte, 4)
		binary.PutVarint(bs, int64(item))
		h.Write(bs)
		output = output ^ h.Sum64()
	}
	return output
}

/*
func LoadStatistics(path string, hg Hypergraph) StatisticsDB {
	// 1. read the csv file
	csvfile, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("can't open %v: %v", path, err))
	}

	// 2. init map
	res := make(StatisticsDB)
	var edgeCombs []lib.Edges

	r := csv.NewReader(csvfile)
	r.FieldsPerRecord = -1
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		// 3. put the record into the map
		last := len(record) - 1
		val, err := strconv.Atoi(record[last])
		if err != nil {
			panic(fmt.Errorf("%v is not an int: %v", record[last], err))
		}
		tag := record[0]
		switch tag {
		case "size":
			rec := record[1:last]
			comb := make([]int, len(rec))
			for p, s := range rec {
				comb[p] = hg.Enc[s]
			}
			edges := selectEdges(hg.Graph, comb)
			if _, ok := res.Stats(edges); !ok {
				attrs := computeAttrs(hg.Graph, comb)
				res.PutOld(edges, db.NewStatistics(attrs))
			}
			st, _ := res.Stats(edges)
			st.SetSize(val)

			if edges.Len() >= 2 {
				edgeCombs = append(edgeCombs, edges)
			}
		case "ndv":
			tabs := record[1 : last-1]
			col := record[last-1]

			comb := make([]int, len(tabs))
			for p, s := range tabs {
				comb[p] = hg.Enc[s]
			}
			edges := selectEdges(hg.Graph, comb)
			v := strconv.Itoa(hg.Enc[col])

			if _, ok := res.Stats(edges); !ok {
				attrs := computeAttrs(hg.Graph, comb)
				res.PutOld(edges, db.NewStatistics(attrs))
			}
			st, _ := res.Stats(edges)
			st.SetNdv(v, val)
		}
	}

	// estimate ndv for combinations of tables
	for _, edges := range edgeCombs {
		combStats, _ := res.Stats(edges)
		oldSize := combStats.Size
		var edgeStats []*db.Statistics
		for _, e := range edges.Slice() {
			eStats, _ := res.Stats(lib.NewEdges([]lib.Edge{e}))
			edgeStats = append(edgeStats, eStats)
		}
		newSize, newCombStats := db.EstimateJoinSize(edgeStats)
		if oldSize != newSize {
			fmt.Println("new size estimate for", edges, ":", oldSize, "->", newSize)
		}
		res.PutOld(edges, newCombStats)
	}

	return res
}

func computeAttrs(graph lib.Graph, comb []int) []string {
	var attrs []string
	attrSet := make(map[int]bool)
	edges := selectEdges(graph, comb)
	for _, e := range edges.Slice() {
		for _, v := range e.Vertices {
			if _, found := attrSet[v]; !found {
				attrSet[v] = true
				attrs = append(attrs, strconv.Itoa(v))
			}
		}
	}
	return attrs
}

func selectEdges(graph lib.Graph, comb []int) lib.Edges {
	var output []lib.Edge
	for _, name := range comb {
		if e, ok := findEdge(graph.Edges, name); ok {
			output = append(output, e)
		} else {
			panic(fmt.Errorf("edge %v missing", name))
		}
	}
	return lib.NewEdges(output)
}

func findEdge(edges lib.Edges, name int) (lib.Edge, bool) {
	for _, e := range edges.Slice() {
		if e.Name == name {
			return e, true
		}
	}
	return lib.Edge{}, false
}

// Put the statistics of an edge combination into the map
func (sdb StatisticsDB) PutOld(edges lib.Edges, stats *db.Statistics) {
	h := hashNames(edges)
	sdb[h] = stats
}

// Statistics of an edge combination
func (sdb StatisticsDB) Stats(edges lib.Edges) (*db.Statistics, bool) {
	h := hashNames(edges)
	if c, ok := sdb[h]; ok {
		return c, true
	}
	return nil, false
}

func hashNames(edges lib.Edges) uint64 {
	var names []int
	for _, e := range edges.Slice() {
		names = append(names, e.Name)
	}

	var output uint64
	for _, item := range names {
		h := fnv.New64a()
		bs := make([]byte, 4)
		binary.PutVarint(bs, int64(item))
		h.Write(bs)
		output = output ^ h.Sum64()
	}

	return output
}
*/
