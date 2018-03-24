package logger

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

var logger *log.Logger
var once sync.Once

func Init() {
	once.Do(func() {
		buf := bufio.NewWriter(os.Stdout)
		logger = log.New(buf, "horlix: ", log.Lshortfile)
	})
}

func LogInfo(msg string) {
	logger.Println(msg)
	fmt.Println(msg)
}

func LogInfof(format string, a ...interface{}) {
	fmt.Printf(format, a...)
}
