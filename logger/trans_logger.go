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
var tRecoveryFlag bool

func InitTransLogger(transLogDir string) error {
	var err error
	transLog, err = os.Create(transLogDir + "/transaction.log")
	if err != nil {
		return err
	}
	tWriter = bufio.NewWriter(transLog)
	return nil
}

func SetTransLogRecoveryFlag() {
	tRecoveryFlag = true
}

func UnSetTransLogRecoveryFlag() {
	tRecoveryFlag = false
}

func TruncateTransLog() {
	transLog.Truncate(0)
	transLog.Seek(0, 0)
}

func LogTransaction(opr string, req contract.IRequestContract) error {
	if tRecoveryFlag {
		// recovering data from trans log. so, not logging the transaction
		return nil
	}
	reqBytes, err := req.Serialize()
	if err != nil {
		return err
	}
	reqString := string(reqBytes[:])
	tWriter.WriteString(opr + "\n" + reqString + "\n")
	if err := tWriter.Flush(); err != nil {
		return err
	}
	// tLogger.Println(opr)
	// tLogger.Println(bytes)
	return nil
}
