package logger

import (
	"bufio"
	"log"
	"os"

	"github.com/ggvishnu29/horlix/contract"
)

var transLog *os.File
var tLogger *log.Logger
var tWriter *bufio.Writer

func InitTransLogger(transLogDir string) error {
	var err error
	transLog, err = os.Create(transLogDir + "/transaction.log")
	if err != nil {
		return err
	}
	// buf := bufio.NewWriter(transLog)
	// tLogger = log.New(buf, "", log.Lshortfile)
	tWriter = bufio.NewWriter(transLog)
	return nil
}

func TruncateTransLog() {
	transLog.Truncate(0)
	transLog.Seek(0, 0)
}

func LogTransaction(opr string, req contract.IRequestContract) error {
	reqBytes, err := req.Serialize()
	if err != nil {
		return err
	}
	reqString := string(reqBytes[:])
	tWriter.WriteString(opr + "\n" + reqString + "\n")
	// tLogger.Println(opr)
	// tLogger.Println(bytes)
	return nil
}
