package main

import (
	"time"
	//"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
	"github.com/ggvishnu29/horlix/worker"
)

var numEnqueues = 0
var numDequeues = 0
var numDeletes = 0
var numReleases = 0
var numAcks = 0
var ackCounter = 0
var releaseCounter = 0
var deleteCounter = 0

func main() {
	logger.AppLogger()
	logger.LogInfo("starting horlix")
	// start http process here
	go worker.StartTubesManager()
	logger.LogInfo("started horlix")
	go testHorlix()
	signalCatcher()
}

func testHorlix() {
	fuseSetting := model.NewFuseSetting(1)
	operation.CreateTube("tube1", 10, fuseSetting)
	go enqueueMsgs()
	go printStats()
	for true {
		msg, err := operation.GetMsg("tube1")
		if err != nil {
			panic(err)
		}
		numDequeues++
		if msg == nil {
			continue
		}
		releaseCounter++
		deleteCounter++
		if releaseCounter == 10000 {
			releaseCounter = 0
			err = operation.ReleaseMsg(msg.Tube.ID, msg.ID, msg.ReceiptID, 5)
			if err != nil {
				panic(err)
			}
			numReleases++
		} else {
			err = operation.AckMsg(msg.Tube.ID, msg.ID, msg.ReceiptID)
			if err != nil {
				panic(err)
			}
			numAcks++
			
		}
		if deleteCounter == 100000 {
			deleteCounter = 0
			err = operation.DeleteMsg(msg.Tube.ID, msg.ID)
			if err != nil {
				panic(err)
			}
			numDeletes++
		}
		//logger.LogInfof("dequeued %v, msg slice size: %v\n", msg.ID, len(msg.Data.DataSlice))
		//b, _ := json.Marshal(msg)
		//logger.LogInfof("msg: %v\n", string(b))
		//err = operation.ReleaseMsg(msg.Tube.ID, msg.ID, msg.ReceiptID, 10)
		//err = operation.AckMsg(msg.Tube.ID, msg.ID, msg.ReceiptID)
		//err = operation.DeleteMsg(msg.Tube.ID, msg.ID)
		//time.Sleep(10 * time.Millisecond)
	}
}

func printStats() {
	for true {
		enqueueNum1 := numEnqueues
		dequeueNum1 := numDequeues
		deleteNum1 := numDeletes
		ackNum1 := numAcks
		releaseNum1 := numReleases
		time.Sleep(1 * time.Second)
		enqueueRate := numEnqueues - enqueueNum1
		dequeueRate := numDequeues - dequeueNum1
		deleteRate := numDeletes - deleteNum1
		ackRate := numAcks - ackNum1
		releaseRate := numReleases - releaseNum1
		tube := model.GetTubeMap().GetTube("tube1")
		logger.LogInfof("enqueue rate: %v dequeue rate: %v delete rate: %v ack rate: %v release rate: %v\n", enqueueRate, dequeueRate, deleteRate, ackRate, releaseRate)
		logger.LogInfof("readyQSize: %v reservedQSize: %v delayedQSize: %v\n", tube.ReadyQueue.Size(), tube.ReservedQueue.Size(), tube.DelayedQueue.Size())
		//logger.LogInfo("Delayed Queue:")
		//tube.DelayedQueue.Print()
	}
}

func enqueueMsgs() {
	for true {
		//i := 1
		// for i <= 1000 {
		// 	msgID := "msg" + strconv.Itoa(i)
		// 	err := operation.PutMsg("tube1", msgID, []byte("hello"), 1, int64(i%10))
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	time.Sleep(1 * time.Second)
		// 	i++
		// 	numEnqueues++
		// }
		err := operation.PutMsg("tube1", "msg1", []byte("hello"), 1, 0)
		if err != nil {
			panic(err)
		}
		numEnqueues++
		//time.Sleep(1 * time.Second)
		// err = operation.PutMsg("tube1", "msg2", []byte("world"), 1, 2)
		// if err != nil {
		// 	panic(err)
		// }
		// numEnqueues++
		// err = operation.PutMsg("tube1", "msg1", []byte("world"), 1, 10)
		// if err != nil {
		// 	panic(err)
		// }
		// numEnqueues++

		//time.Sleep(3 * time.Second)
		// if numEnqueues > 10000000 {
		// 	logger.LogInfo("stop enqueueing.......")
		// 	break
		// }
	}
}

func signalCatcher() {
	// Go signal notification works by sending `os.Signal`
	// values on a channel. We'll create a channel to
	// receive these notifications
	sigs := make(chan os.Signal, 1)

	// `signal.Notify` registers the given channel to
	// receive notifications of the specified signals.
	signal.Notify(sigs,
		syscall.SIGINT,
		syscall.SIGTERM,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGQUIT)

	// Following piece of code executes a blocking receive for
	// signals and invokes shutdownServices()
	select {
	case <-sigs:
	}
	logger.LogInfo("exiting !!!!!!")
	os.Exit(0)
}
