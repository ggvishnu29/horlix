package logger

import (
	"bufio"
	"log"
	"os"
	"sync"

	"github.com/ggvishnu29/horlix/contract"
)

var transLog *os.File
var tLogger *log.Logger
var o2 sync.Once

func InitTransLogger(transLogDir string) error {
	var err error
	transLog, err = os.Create(transLogDir + "/transaction.log")
	if err != nil {
		return err
	}
	buf := bufio.NewWriter(transLog)
	tLogger = log.New(buf, "", log.Lshortfile)
	return nil
}

func TruncateTransLog() {
	transLog.Truncate(0)
	transLog.Seek(0, 0)
}

func LogTransaction(opr string, req contract.IRequestContract) error {
	bytes, err := req.Serialize()
	if err != nil {
		return err
	}
	tLogger.Println(opr)
	tLogger.Println(bytes)
	return nil
}
