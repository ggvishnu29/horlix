package logger

import (
	"bufio"
	"io"
	"log"
	"os"
)

var transLog *os.File
var tLogger *log.Logger
var tWriter *bufio.Writer
var tRecoveryFlag bool
var transLogPath string

func InitTransLogger(transLogDir string) error {
	var err error
	transLogPath = transLogDir + "/transaction.log"
	transLog, err = os.Create(transLogPath)
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

func CopyTransLogToFile(file string) error {
	in, err := os.Open(transLogPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(file)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return nil
}

func TruncateTransLog() {
	transLog.Truncate(0)
	transLog.Seek(0, 0)
}

func LogTransactions(logs [][]byte) error {
	for _, log := range logs {
		if _, err := tWriter.WriteString(string(log[:]) + "\n"); err != nil {
			return err
		}
	}
	if err := tWriter.Flush(); err != nil {
		return err
	}
	return nil
}
