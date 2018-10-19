package logger

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func createTempDir() string {
	dir, err := ioutil.TempDir("/tmp", "translog")
	if err != nil {
		panic(err)
	}
	return dir
}

func TestWriteTranslog(t *testing.T) {
	transLogDir := createTempDir()
	defer os.RemoveAll(transLogDir)
	tLogger, err := NewTransLogger(transLogDir, 1)
	if err != nil {
		panic(err)
	}
	logs := [][]byte{[]byte("test"), []byte("test")}
	tLogger.WriteTransLogs(logs)
	tLogger.CloseTransLog()
	fileNames, err := tLogger.ListTransLogFiles()
	if err != nil {
		panic(err)
	}
	assert.Equal(t, 1, len(fileNames))
	logs, err = tLogger.ReadTransLog(fileNames[0])
	if err != nil {
		panic(err)
	}
	assert.Equal(t, 2, len(logs))
	assert.Equal(t, "test", string(logs[0][:]))
	assert.Equal(t, "test", string(logs[1][:]))
	tLogger.DeleteTransLog(fileNames[0])
	fileNames, err = tLogger.ListTransLogFiles()
	if err != nil {
		panic(err)
	}
	assert.Equal(t, 0, len(fileNames))
}

func TestWriteMoreThan100MB(t *testing.T) {
	transLogDir := createTempDir()
	defer os.RemoveAll(transLogDir)
	tLogger, err := NewTransLogger(transLogDir, 1)
	if err != nil {
		panic(err)
	}
	logs := [][]byte{[]byte("xxxxxxxxxxxxxxx"), []byte("yyyyyyyyyyyyyyyyyy")}
	for i := 1; i <= 100000; i++ {
		tLogger.WriteTransLogs(logs)
	}
	tLogger.CloseTransLog()
	fileNames, err := tLogger.ListTransLogFiles()
	if err != nil {
		panic(err)
	}
	assert.Equal(t, 4, len(fileNames))
}
