package logger

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

const transLogPrefix = "translog."

type TransLogger interface {
	WriteTransLogs(logs [][]byte) error
	CloseTransLog() error
	ListTransLogFiles() ([]string, error)
	ReadTransLog(fileName string) ([][]byte, error)
	DeleteTransLog(fileName string) error
}

type TransLoggerImpl struct {
	transLogFile   *os.File
	maxFileSizeMB  int64
	transLogDir    string
	transLogWriter *bufio.Writer
}

func NewTransLogger(transLogDir string, maxFileSizeMB int64) (*TransLoggerImpl, error) {
	tFile, err := getNewTransLogFile(transLogDir)
	if err != nil {
		return nil, err
	}
	tLoggerImpl := &TransLoggerImpl{
		transLogFile:   tFile,
		maxFileSizeMB:  maxFileSizeMB,
		transLogDir:    transLogDir,
		transLogWriter: bufio.NewWriter(tFile),
	}
	return tLoggerImpl, nil
}

func getNewTransLogFile(transLogDir string) (*os.File, error) {
	curTime := time.Now().UnixNano()
	transLogFilePath := transLogDir + "/" + transLogPrefix + strconv.Itoa(int(curTime))
	transLogFile, err := os.Create(transLogFilePath)
	if err != nil {
		return nil, fmt.Errorf("error while creating new translog file %v. %v", transLog.Name(), err)
	}
	return transLogFile, nil
}

func (t *TransLoggerImpl) WriteTransLogs(logs [][]byte) error {
	stats, err := t.transLogFile.Stat()
	if err != nil {
		return fmt.Errorf("error while reading stats of translog file: %v. %v", t.transLogFile.Name(), err)
	}
	if stats.Size() >= t.maxFileSizeMB*1000*1000 {
		err := t.transLogFile.Close()
		if err != nil {
			return fmt.Errorf("error while closing trans log file: %v. %v", t.transLogFile.Name(), err)
		}
		tFile, err := getNewTransLogFile(t.transLogDir)
		if err != nil {
			return err
		}
		t.transLogFile = tFile
		t.transLogWriter = bufio.NewWriter(t.transLogFile)
	}
	tWriter := t.transLogWriter
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

func (t *TransLoggerImpl) CloseTransLog() error {
	if t.transLogFile != nil {
		return t.transLogFile.Close()
	}
	return nil
}

func (t *TransLoggerImpl) ListTransLogFiles() ([]string, error) {
	fileInfos, err := ioutil.ReadDir(t.transLogDir)
	if err != nil {
		return nil, fmt.Errorf("error while listing translog directory: %v", err)
	}
	var fileNames []string
	for _, fileName := range fileInfos {
		fileNames = append(fileNames, fileName.Name())
	}
	return fileNames, nil
}

func (t *TransLoggerImpl) ReadTransLog(fileName string) ([][]byte, error) {
	tFile, err := os.Open(t.transLogDir + "/" + fileName)
	if err != nil {
		return nil, fmt.Errorf("error while reading trans log file: %v. %v", fileName, err)
	}
	defer tFile.Close()
	scanner := bufio.NewScanner(tFile)
	var logLines [][]byte
	for scanner.Scan() {
		logLines = append(logLines, scanner.Bytes())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error while reading trans log file: %v. %v", fileName, err)
	}
	return logLines, nil
}

func (t *TransLoggerImpl) DeleteTransLog(fileName string) error {
	err := os.Remove(t.transLogDir + "/" + fileName)
	if err != nil {
		return fmt.Errorf("error while deleting trans log file: %v, %v", fileName, err)
	}
	return nil
}
