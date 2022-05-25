package decomp

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/callidus/db"
)

type Hypergraph struct {
	Graph   lib.Graph
	Filters []Filter
	Out     []int

	Enc map[string]int
	Dec map[int]string
}

type Filter struct {
	Verts []int
	//e lib.Edge
	cond db.Condition
}

// ReadHypergraph from a Reader
func ReadHypergraph(r io.Reader) Hypergraph {
	dat, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	hg, parsedGraph := lib.GetGraph(string(dat))

	dec := make(map[int]string)
	for k, v := range parsedGraph.Encoding {
		dec[v] = k
	}

	return Hypergraph{
		Graph:   hg,
		Filters: nil,
		Out:     []int{},
		Enc:     parsedGraph.Encoding,
		Dec:     dec,
	}
}

// MakeHypergraphs
func MakeHypergraph(graph string, filters string, out string) Hypergraph {
	outHg, pOutHg := lib.GetGraph(out)
	if outHg.Edges.Len() > 1 {
		panic(fmt.Errorf("too many out edges %v: ", pOutHg.Edges))
	}

	hg, parsedGraph := lib.GetGraph(graph)

	dec := make(map[int]string)
	for k, v := range parsedGraph.Encoding {
		dec[v] = k
	}

	// TODO deal with filters

	var outVerts []int
	for _, v := range pOutHg.Edges[0].Vertices {
		outVerts = append(outVerts, parsedGraph.Encoding[v])
	}

	return Hypergraph{
		Graph:   hg,
		Filters: nil,
		Out:     outVerts,
		Enc:     parsedGraph.Encoding,
		Dec:     dec,
	}
}

func (hg Hypergraph) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i, e := range hg.Graph.Edges.Slice() {
		eName := hg.Dec[e.Name] // unsafe: no locks used
		buffer.WriteString(eName)
		if i != hg.Graph.Edges.Len()-1 {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString("}")

	return buffer.String()
}

func Decompose(hg Hypergraph, evl Evaluator) Hypertree {
	return *DecompH(hg, evl)
}

func GetGMLString(hd Hypertree) string {
	return toCemDecomp(hd).ToGML()
}

func GetCemString(hd Hypertree) string {
	return toCemDecomp(hd).String()
}

func toCemDecomp(hd Hypertree) lib.Decomp {
	return lib.Decomp{
		Root: makeCemDecomp(hd),
	}
}

func makeCemDecomp(s Hypertree) lib.Node {
	n := lib.Node{Bag: s.Bag, Cover: s.Cover}
	var subtrees []lib.Node
	for _, c := range s.Children {
		subtrees = append(subtrees, makeCemDecomp(*c))
	}
	n.Children = subtrees
	return n
}
