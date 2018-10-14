package worker

import (
	"time"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/serde"
)

type TransLogWorker struct {
	serde serde.ISerde
}

func NewTransLogWorker(s serde.ISerde) *TransLogWorker {
	return &TransLogWorker{
		serde: s,
	}
}

func (l *TransLogWorker) StartTransLogWorker() {
	var logLines [][]byte
	timeout := time.After(1 * time.Second)
	for {
		select {
		case <-timeout:
			logger.LogTransactions(logLines)
			logLines = [][]byte{}
			timeout = time.After(1 * time.Second)
		case opr := <-model.LogWorkerChan:
			bytes, err := l.serde.Serialize(opr)
			if err != nil {
				logger.LogFatal("error serializing operation to trans log. " + err.Error())
				panic(err)
			}
			logLines = append(logLines, bytes)
		default:
			// sleeping for sometime before starting next iteration to avoid unnecessary cpu cycles
			time.Sleep(100 * time.Millisecond)
		}
	}
}
