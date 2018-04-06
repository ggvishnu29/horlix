package logger

import (
	"bufio"
	"log"
	"os"
	"sync"
)

var tLogger *log.Logger
var o2 sync.Once

func InitTransLogger() {
	o2.Do(func() {
		buf := bufio.NewWriter(os.Stdout)
		tLogger = log.New(buf, "", log.Lshortfile)
	})
}

func LogTransaction(opr string, args ...interface{}) {
	l := append(make([]string, 1), opr)
	for _, arg := range args {
		l = append(l, ",", arg)
	}
}