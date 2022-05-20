package xcsp

import "testing"

func TestMakeVarList1(t *testing.T) {
	vars := []string{"t", "rL0J", "sL3JL4J", "rtL3", "ssLJ4", "LJ", "", "tLrJ", "svnL18JL4J", "L3J", "naLmeL7J", "nLjJL6J"}
	res := makeVarList(vars)
	expected := []string{"t", "r[0]", "s[3][4]", "rtL3", "ssLJ4", "LJ", "", "tLrJ", "svn[18][4]", "L3J", "naLme[7]", "nLjJ[6]"}

	if len(res) == 0 {
		t.Errorf("res empty; want %q", expected)
	}
	for i, v := range res {
		if v != expected[i] {
			t.Errorf("res[%d]= %s; want %s", i, v, expected[i])
		}
	}
}

func TestMakeVarList2(t *testing.T) {
	vars := []string{"t", "rL0J", "rL1J", "st", "yL1J", "yL2J", "yL3J", "z"}
	res := makeVarList(vars)
	expected := []string{"t", "r[0]", "r[1]", "st", "y[1]", "y[2]", "y[3]", "z"}

	if len(res) == 0 {
		t.Errorf("res empty; want %q", expected)
	}
	for i, v := range res {
		if v != expected[i] {
			t.Errorf("res[%d]= %s; want %s", i, v, expected[i])
		}
	}
}

func TestMakeVarListCompressed1(t *testing.T) {
	vars := []string{"t", "rL0J", "sL3JL4J", "rtL3", "ssLJ4", "LJ", "", "tLrJ", "svnL18JL4J", "L3J", "naLmeL7J", "nLjJL6J"}
	res := makeVarListCompressed(vars)
	expected := []string{"t", "r[]", "s[][]", "rtL3", "ssLJ4", "LJ", "", "tLrJ", "svn[][]", "L3J", "naLme[]", "nLjJ[]"}

	if len(res) == 0 {
		t.Errorf("res empty; want %q", expected)
	}
	for i, v := range res {
		if v != expected[i] {
			t.Errorf("res[%d]= %s; want %s", i, v, expected[i])
		}
	}
}

func TestMakeVarListCompressed2(t *testing.T) {
	vars := []string{"t", "rL0J", "rL1J", "st", "yL1J", "yL2J", "yL3J", "z"}
	res := makeVarListCompressed(vars)
	expected := []string{"t", "r[]", "st", "y[]", "z"}

	if len(res) == 0 {
		t.Errorf("res empty; want %q", expected)
	}
	for i, v := range res {
		if v != expected[i] {
			t.Errorf("res[%d]= %s; want %s", i, v, expected[i])
		}
	}
}
