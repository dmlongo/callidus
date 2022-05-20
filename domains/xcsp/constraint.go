package xcsp

import "strings"

// Constraint is an interface for XCSP3 constraints
type Constraint interface {
	Name() string
	Variables() []string
	ToXCSP() []string
}

// primitiveCtr represents a primitive constraint in XCSP
type primitiveCtr struct {
	CName    string
	Vars     string
	strVars  []string
	Function string
}

// Name of this constraint
func (c *primitiveCtr) Name() string {
	return c.CName
}

// Variables of this constraint
func (c *primitiveCtr) Variables() []string {
	if c.strVars != nil {
		return c.strVars
	}
	c.strVars = append(c.strVars, strings.Split(c.Vars, " ")...)
	return c.strVars
}

// ToXCSP converts this constraint in the XCSP format
func (c *primitiveCtr) ToXCSP() []string {
	return []string{"<intension> " + c.Function + " </intension>"}
}

// extensionCtr represents an extensional constraint in XCSP
type extensionCtr struct {
	CName   string
	Vars    string
	strVars []string
	CType   string
	Tuples  string
}

// Name of this constraint
func (c *extensionCtr) Name() string {
	return c.CName
}

// Variables of this constraint
func (c *extensionCtr) Variables() []string {
	if c.strVars != nil {
		return c.strVars
	}
	c.strVars = append(c.strVars, strings.Split(c.Vars, " ")...)
	return c.strVars
}

// ToXCSP converts this constraint in the XCSP format
func (c *extensionCtr) ToXCSP() []string {
	out := make([]string, 0, 4)
	out = append(out, "<extension>")
	out = append(out, "\t<list> "+c.Vars+" </list>")
	out = append(out, "\t<"+c.CType+"> "+c.Tuples+" </"+c.CType+">")
	out = append(out, "</extension>")
	return out
}

// allDifferentCtr represents an allDifferent constraint in XCSP
type allDifferentCtr struct {
	CName   string
	Vars    string
	strVars []string
}

// Name of this constraint
func (c *allDifferentCtr) Name() string {
	return c.CName
}

// Variables of this constraint
func (c *allDifferentCtr) Variables() []string {
	if c.strVars != nil {
		return c.strVars
	}
	c.strVars = append(c.strVars, strings.Split(c.Vars, " ")...)
	return c.strVars
}

// ToXCSP converts this constraint in the XCSP format
func (c *allDifferentCtr) ToXCSP() []string {
	return []string{"<allDifferent> " + c.Vars + " </allDifferent>"}
}

// sumCtr represents a sum constraint in XCSP
type sumCtr struct {
	CName     string
	Vars      string
	strVars   []string
	Coeffs    string
	Condition string
}

// Name of this constraint
func (c *sumCtr) Name() string {
	return c.CName
}

// Variables of this constraint
func (c *sumCtr) Variables() []string {
	if c.strVars != nil {
		return c.strVars
	}
	c.strVars = append(c.strVars, strings.Split(c.Vars, " ")...)
	return c.strVars
}

// ToXCSP converts this constraint in the XCSP format
func (c *sumCtr) ToXCSP() []string {
	out := make([]string, 0, 5)
	out = append(out, "<sum>")
	out = append(out, "\t<list> "+c.Vars+" </list>")
	out = append(out, "\t<coeffs> "+c.Coeffs+" </coeffs>")
	out = append(out, "\t<condition> "+c.Condition+" </condition>")
	out = append(out, "</sum>")
	return out
}

// elementCtr represents an element constraint in XCSP
type elementCtr struct {
	CName      string
	Vars       string
	strVars    []string
	List       string
	StartIndex string
	Index      string
	Rank       string
	Condition  string
}

// Name of this constraint
func (c *elementCtr) Name() string {
	return c.CName
}

// Variables of this constraint
func (c *elementCtr) Variables() []string {
	if c.strVars != nil {
		return c.strVars
	}
	c.strVars = append(c.strVars, strings.Split(c.Vars, " ")...)
	return c.strVars
}

// ToXCSP converts this constraint in the XCSP format
func (c *elementCtr) ToXCSP() []string {
	out := make([]string, 0, 5)
	out = append(out, "<element>")
	out = append(out, "\t<list startIndex=\""+c.StartIndex+"\"> "+c.List+" </list>")
	out = append(out, "\t<index rank=\""+c.Rank+"\"> "+c.Index+" </index>")
	out = append(out, "\t<value> "+c.Condition+" </value>")
	out = append(out, "</element>")
	return out
}
