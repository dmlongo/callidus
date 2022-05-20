package xcsp

import (
	"bufio"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/dmlongo/callidus/solver"
)

// TODO maybe instead of writing to file, I could just return strings (for in-memory reasons)

// CreateXCSPInstance from given constraints
func CreateXCSPInstance(constraints []Constraint, variables map[string]string, outFile string) {
	file, err := os.Create(outFile)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	w := bufio.NewWriter(file)
	_, err = w.WriteString("<instance format=\"XCSP3\" type=\"CSP\">\n")
	if err != nil {
		panic(err)
	}
	writeVariables(w, variables)
	writeConstraints(w, constraints)
	_, err = w.WriteString("</instance>\n")
	if err != nil {
		panic(err)
	}
	w.Flush()
}

func writeVariables(w *bufio.Writer, variables map[string]string) {
	_, err := w.WriteString("\t<variables>\n")
	if err != nil {
		panic(err)
	}
	var vars []string
	for v := range variables {
		vars = append(vars, v)
	}
	sort.Strings(vars)
	for _, v := range vars {
		w.WriteString("\t\t<var id=\"" + v + "\"> ")
		if err != nil {
			panic(err)
		}
		w.WriteString(variables[v])
		if err != nil {
			panic(err)
		}
		w.WriteString(" </var>\n")
		if err != nil {
			panic(err)
		}
	}
	_, err = w.WriteString("\t</variables>\n")
	if err != nil {
		panic(err)
	}
}

func writeConstraints(w *bufio.Writer, constraints []Constraint) {
	_, err := w.WriteString("\t<constraints>\n")
	if err != nil {
		panic(err)
	}
	for _, c := range constraints {
		for _, line := range c.ToXCSP() {
			_, err := w.WriteString("\t\t" + line + "\n")
			if err != nil {
				panic(err)
			}
		}
	}
	_, err = w.WriteString("\t</constraints>\n")
	if err != nil {
		panic(err)
	}
}

// WriteSolution in XCSP format
func WriteSolution(sol solver.Solution) string {
	vars := sol.SortVars()
	varList := makeVarList(vars)

	var sb strings.Builder
	sb.WriteString("<instantiation>\n")
	sb.WriteString("\t<list> ")
	for _, v := range varList {
		sb.WriteString(v)
		sb.WriteString(" ")
	}
	sb.WriteString("</list>\n")
	sb.WriteString("\t<values> ")
	for _, v := range vars {
		val := sol[v]
		sb.WriteString(val) //strconv.Itoa(val))
		sb.WriteString(" ")
	}
	sb.WriteString("</values>\n")
	sb.WriteString("</instantiation>\n")
	return sb.String()

}

var arrayIDRegex = regexp.MustCompile(`^((\w)+?)((L\d+J)+)$`)
var indicesRegex = regexp.MustCompile(`L\d+J`)

func makeVarList(sortedVars []string) []string {
	var list []string

	var sb strings.Builder
	for _, v := range sortedVars {
		if tks := arrayIDRegex.FindStringSubmatch(v); tks != nil {
			varName := tks[1]
			sb.WriteString(varName)
			indices := tks[3]
			indices = strings.ReplaceAll(indices, "L", "[")
			indices = strings.ReplaceAll(indices, "J", "]")
			sb.WriteString(indices)
			list = append(list, sb.String())
			sb.Reset()
		} else {
			list = append(list, v)
		}
	}

	return list
}

func makeVarListCompressed(sortedVars []string) []string {
	var list []string

	var sb strings.Builder
	for _, v := range sortedVars {
		if arrayIDRegex.MatchString(v) {
			tks := arrayIDRegex.FindStringSubmatch(v)
			name := tks[1]
			if list == nil || !strings.HasPrefix(list[len(list)-1], name+"[") {
				sb.WriteString(name)
				for range indicesRegex.FindAllString(v, -1) {
					sb.WriteString("[]")
				}
				list = append(list, sb.String())
				sb.Reset()
			}
		} else {
			list = append(list, v)
		}
	}

	return list
}
