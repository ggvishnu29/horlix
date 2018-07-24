package worker

import (
	"time"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/serde"
)

type LogWorker struct {
	serde serde.ISerde
}

func NewLogWorker(s serde.ISerde) *LogWorker {
	return &LogWorker{
		serde: s,
	}
}

func (l *LogWorker) StartLogWorker() {
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
