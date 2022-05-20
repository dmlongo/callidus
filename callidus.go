package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/dmlongo/callidus/db"
	"github.com/dmlongo/callidus/decomp"
	"github.com/dmlongo/callidus/domains"
	"github.com/dmlongo/callidus/domains/datalog"
	"github.com/dmlongo/callidus/domains/xcsp"
	"github.com/dmlongo/callidus/solver"
	"github.com/dmlongo/callidus/solver/nacre"
)

var cspIn string

//var queryIn, dbIn string
//var domainIn, taskIn string

var ruleIn, factsIn string

var instType string

var qInstance string
var dbInstance string

var conv domains.Converter

//var solv solver.Solver

var start time.Time
var durs []time.Duration

var gml string
var wrkdir string

var instDir string
var instName string
var baseDir string

var onlyDecomp bool
var printSol bool

func main() {
	setFlags()

	fmt.Printf("Callidus starts solving %s!\n", qInstance)
	start = time.Now()

	fmt.Print("Creating hypergraph... ")
	startConvHg := time.Now()
	hg := conv.ToHypergraph(qInstance)
	durConvHg := time.Since(startConvHg)
	durs = append(durs, durConvHg)
	fmt.Println("done in", durConvHg)

	fmt.Print("Creating database... ")
	startConvDb := time.Now()
	db := conv.ToDatabase(dbInstance)
	durConvDb := time.Since(startConvDb)
	durs = append(durs, durConvDb)
	fmt.Println("done in", durConvDb)

	evl := initEvaluator(conv, hg, db)

	fmt.Print("Coming up with a plan... ")
	startDecomp := time.Now()
	hd := decomp.Decompose(hg, evl)
	durDecomp := time.Since(startDecomp)
	durs = append(durs, durDecomp)
	fmt.Println("done in", durDecomp)

	if onlyDecomp {
		gmlRepr := decomp.GetGMLString(hd)
		fmt.Println()
		fmt.Println(gmlRepr)

		if gml != "" {
			f, err := os.Create(gml)
			if err != nil {
				panic(err)
			}

			defer f.Close()
			f.WriteString(gmlRepr)
			f.Sync()
		}
		return
	}

	solv := initSolver(conv, hg)

	fmt.Print("Solving instance... ")
	startSol := time.Now()
	plan := solv.MakePlan(hd)
	sols := solv.Solve(plan, db)
	durSol := time.Since(startSol)
	durs = append(durs, durSol)
	fmt.Println("done in", durSol)

	durCallidus := time.Since(start)
	fmt.Println(qInstance, "solved in", durCallidus)

	if printSol {
		fmt.Println()
		if len(sols) > 0 {
			fmt.Println("Callidus found", len(sols), "solutions:")
			startPrinting := time.Now()
			for sol := range sols {
				sol.Print()
			}
			fmt.Println("Printing done in", time.Since(startPrinting))
		} else {
			fmt.Println(cspIn, "has no solutions")
		}
	}
}

func initEvaluator(conv domains.Converter, hg decomp.Hypergraph, data db.Database) decomp.Evaluator {
	switch instType {
	case "csp":
		xconv := conv.(*xcsp.HgtConverter)
		doms := make(map[int]int)
		for vv, dom := range xconv.Doms {
			if v, ok := xconv.Hg.Enc[vv]; !ok {
				panic(fmt.Errorf("cannot find encoding for %v", vv))
			} else {
				size := strings.Count(dom, " ") + 1
				doms[v] = size
			}
		}
		return &decomp.TrivialGrndEval{Doms: doms}
	case "rule":
		if factsIn == "" {
			return &decomp.NumNodes{} // todo TreeWidthEval
		}
		return decomp.NewNaiveGrndEval(data, hg)
	default:
		return nil
	}
}

func initSolver(conv domains.Converter, hg decomp.Hypergraph) solver.Solver {
	switch instType {
	case "csp":
		xconv := conv.(*xcsp.HgtConverter)
		return nacre.NewSolver(baseDir, xconv.Ctrs, xconv.Doms, hg.Dec)
	default:
		return nil
	}
}

func setFlags() {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	flagSet.SetOutput(ioutil.Discard) //todo: see what happens without this line

	flagSet.StringVar(&cspIn, "csp", "", "Path to the CSP to solve (XCSP3 format)")
	flagSet.StringVar(&ruleIn, "rule", "", "Path to the Datalog rule to decompose")
	flagSet.StringVar(&factsIn, "facts", "", "Path to the Datalog facts (used with -rule)")

	flagSet.StringVar(&wrkdir, "wrkdir", "wrkdir", "Path to the working directory")
	flagSet.StringVar(&gml, "gml", "", "Write decomposition into the specified file")
	flagSet.BoolVar(&onlyDecomp, "onlyDecomp", false, "Decompose the instance without solving it")
	flagSet.BoolVar(&printSol, "printSol", false, "Print solutions of the instance")

	parseError := flagSet.Parse(os.Args[1:])
	if parseError != nil {
		fmt.Print("Parse Error:\n", parseError.Error(), "\n\n")
	}

	if parseError != nil || !flagsUsedCorrectly() {
		out := "Usage of Callidus (https://github.com/dmlongo/Callidus)\n"
		flagSet.VisitAll(func(f *flag.Flag) {
			if f.Name != "csp" && (f.Name != "rule" && f.Name != "facts") {
				return
			}
			s := fmt.Sprintf("%T", f.Value) // used to get type of flag
			if s[6:len(s)-5] != "bool" {
				out += fmt.Sprintf("  -%-10s \t<%s>\n", f.Name, s[6:len(s)-5])
			} else {
				out += fmt.Sprintf("  -%-10s \n", f.Name)
			}
			out += fmt.Sprintln("\t" + f.Usage)
		})
		out += fmt.Sprintln("\nOptional Arguments: ")
		flagSet.VisitAll(func(f *flag.Flag) {
			if f.Name == "csp" || (f.Name == "rule" || f.Name == "facts") {
				return
			}
			s := fmt.Sprintf("%T", f.Value) // used to get type of flag
			if s[6:len(s)-5] != "bool" {
				out += fmt.Sprintf("  -%-10s \t<%s>\n", f.Name, s[6:len(s)-5])
			} else {
				out += fmt.Sprintf("  -%-10s \n", f.Name)
			}
			out += fmt.Sprintln("\t" + f.Usage)
		})
		fmt.Fprintln(os.Stderr, out)

		os.Exit(1)
	}

	if cspIn != "" {
		re := regexp.MustCompile(`.*/`)
		instName = re.ReplaceAllString(cspIn, "")
		re = regexp.MustCompile(`\..*`)
		instDir = re.ReplaceAllString(instName, "")
		baseDir = wrkdir + "/" + instDir + "/"

		instType = "csp"

		qInstance = cspIn
		dbInstance = baseDir + instName + ".db"
		conv = xcsp.NewConverter(baseDir, instName)
	} else if ruleIn != "" {
		re := regexp.MustCompile(`.*/`)
		instName = re.ReplaceAllString(ruleIn, "")
		re = regexp.MustCompile(`\..*`)
		instDir = re.ReplaceAllString(instName, "")
		baseDir = wrkdir + "/" + instDir + "/"

		instType = "rule"

		qInstance = ruleIn
		dbInstance = factsIn
		conv = datalog.NewConverter(baseDir, instName, dbInstance)
	}
}

func flagsUsedCorrectly() bool {
	flags := []string{cspIn, ruleIn, factsIn}
	usedFlags := 0
	for _, f := range flags {
		if f != "" {
			usedFlags++
		}
	}

	switch usedFlags {
	case 1:
		return cspIn != "" || ruleIn != ""
	case 2:
		return ruleIn != "" && factsIn != ""
	default:
		return false
	}
}
