package xcsp

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/dmlongo/callidus/files"
)

// ParseConstraints of a CSP
func ParseConstraints(ctrFile string) map[string]Constraint {
	file, err := os.Open(ctrFile)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	reader := bufio.NewReader(file)
	constraints := make(map[string]Constraint)
	numLines := 0
	for {
		line, eof := files.ReadLineCount(reader, &numLines)
		if eof {
			break
		}

		var name string
		var constr Constraint
		switch line {
		case "ExtensionCtr":
			name, _ = files.ReadLineCount(reader, &numLines)
			vars, _ := files.ReadLineCount(reader, &numLines)
			ctype, _ := files.ReadLineCount(reader, &numLines)
			tuples, _ := files.ReadLineCount(reader, &numLines)
			constr = &extensionCtr{CName: name, Vars: vars, CType: ctype, Tuples: tuples}
		case "PrimitiveCtr":
			name, _ = files.ReadLineCount(reader, &numLines)
			vars, _ := files.ReadLineCount(reader, &numLines)
			f, _ := files.ReadLineCount(reader, &numLines)
			constr = &primitiveCtr{CName: name, Vars: vars, Function: f}
		case "AllDifferentCtr":
			name, _ = files.ReadLineCount(reader, &numLines)
			vars, _ := files.ReadLineCount(reader, &numLines)
			constr = &allDifferentCtr{CName: name, Vars: vars}
		case "ElementCtr":
			name, _ = files.ReadLineCount(reader, &numLines)
			vars, _ := files.ReadLineCount(reader, &numLines)
			list, _ := files.ReadLineCount(reader, &numLines)
			startIndex, _ := files.ReadLineCount(reader, &numLines)
			index, _ := files.ReadLineCount(reader, &numLines)
			rank, _ := files.ReadLineCount(reader, &numLines)
			condition, _ := files.ReadLineCount(reader, &numLines)
			constr = &elementCtr{CName: name, Vars: vars, List: list, StartIndex: startIndex, Index: index, Rank: rank, Condition: condition}
		case "SumCtr":
			name, _ = files.ReadLineCount(reader, &numLines)
			vars, _ := files.ReadLineCount(reader, &numLines)
			coeffs, _ := files.ReadLineCount(reader, &numLines)
			condition, _ := files.ReadLineCount(reader, &numLines)
			constr = &sumCtr{CName: name, Vars: vars, Coeffs: coeffs, Condition: condition}
		default:
			msg := ctrFile + ", line " + strconv.Itoa(numLines) + ": " + line + " not implemented yet"
			panic(msg)
		}
		constraints[name] = constr
	}

	return constraints
}

// ParseDomains of CSP variables
func ParseDomains(domFile string) map[string]string {
	file, err := os.Open(domFile)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	reader := bufio.NewReader(file)
	m := make(map[string]string)
	for {
		line, eof := files.ReadLine(reader)
		if eof {
			break
		}

		tks := strings.Split(line, ";")
		variable := tks[0]
		domain := tks[1]
		m[variable] = domain
	}

	return m
}
