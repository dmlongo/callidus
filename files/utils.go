package files

import (
	"bufio"
	"io"
	"os"
	"strings"
)

func ReadLine(r *bufio.Reader) (string, bool) {
	line, err := r.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			if len(line) == 0 {
				return "", true
			} else {
				panic("EOF, but line= " + line)
			}
		} else {
			panic(err)
		}
	}
	return strings.TrimSuffix(line, "\n"), false
}

func ReadLineCount(r *bufio.Reader, n *int) (string, bool) {
	line, eof := ReadLine(r)
	if !eof {
		*n = *n + 1
	}
	return line, eof
}

func MakeDir(name string) string {
	err := os.RemoveAll(name)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(name, 0777)
	if err != nil {
		panic(err)
	}
	return name
}
