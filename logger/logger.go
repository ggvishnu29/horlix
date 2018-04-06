package logger

import (
	"bufio"
	"log"
	"os"
	"sync"
)

var aLogger *log.Logger
var o1 sync.Once

func InitAppLogger() {
	o1.Do(func() {
		buf := bufio.NewWriter(os.Stdout)
		aLogger = log.New(buf, "horlix: ", log.Lshortfile)
	})
}

func LogInfo(msg string) {
	aLogger.Println(msg)
	//fmt.Println(msg)
}

func LogInfof(format string, a ...interface{}) {
	aLogger.Printf(format, a...)
	//fmt.Printf(format, a...)
}
