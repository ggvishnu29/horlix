package main

import (
	"time"

	"os"
	"os/signal"
	"syscall"

	"github.com/ggvishnu29/horlix/contract"
	"github.com/ggvishnu29/horlix/model"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/operation"
	"github.com/ggvishnu29/horlix/serde"
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
	logger.InitAppLogger("/tmp")
	logger.LogInfo("starting horlix")
	logger.InitTransLogger("/tmp")
	serde := &serde.JSONSerde{}
	logWorker := worker.NewTransLogWorker(serde)
	go logWorker.StartTransLogWorker()
	worker.InitSnapshotRecovery("/tmp")
	go worker.StartTubesManager()
	if err := worker.RecoverFromTransLog(); err != nil {
		panic(err)
	}
	go worker.StartSnapshotter()
	logger.LogInfo("started horlix")
	// start http process here
	//rest.Init()
	go testHorlix()
	signalCatcher()
}

func testHorlix() {
	req := &contract.CreateTubeRequest{
		TubeName:            "tube1",
		ReserveTimeoutInSec: 10,
		DataFuseSetting:     1,
	}
	if err := operation.CreateTube(req); err != nil {
		//panic(err)
	}
	go enqueueMsgs()
	//go printStats()
	for true {
		time.Sleep(1 * time.Second)
		req := &contract.GetMsgRequest{
			TubeID: "tube1",
		}
		msg, err := operation.GetMsg(req)
		if err != nil {
			panic(err)
		}
		if msg == nil {
			logger.LogInfo("no msg dequeued")
			continue
		}
		time.Sleep(5 * time.Second)
		numDequeues++
		releaseCounter++
		deleteCounter++
		if releaseCounter == 10 {
			releaseCounter = 0
			req := &contract.ReleaseMsgRequest{
				TubeID:     model.TMap.Tubes[msg.TubeName].ID,
				MsgID:      msg.ID,
				ReceiptID:  msg.ReceiptID,
				DelayInSec: 5,
			}
			err = operation.ReleaseMsg(req)
			if err != nil {
				panic(err)
			}
			numReleases++
		} else {
			req := &contract.AckMsgRequest{
				TubeID:    model.TMap.Tubes[msg.TubeName].ID,
				MsgID:     msg.ID,
				ReceiptID: msg.ReceiptID,
			}
			logger.LogInfo("acked the msg")
			err = operation.AckMsg(req)
			if err != nil {
				panic(err)
			}
			numAcks++
		}
		if deleteCounter == 20 {
			deleteCounter = 0
			req := &contract.DeleteMsgRequest{
				TubeID: model.TMap.Tubes[msg.TubeName].ID,
				MsgID:  msg.ID,
			}
			err = operation.DeleteMsg(req)
			if err != nil {
				panic(err)
			}
			numDeletes++
		}
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
	}
}

func enqueueMsgs() {
	for true {
		req := &contract.PutMsgRequest{
			TubeID:     "tube1",
			MsgID:      "msg1",
			DataBytes:  []byte("hello world"),
			DelayInSec: 0,
			Priority:   0,
		}
		err := operation.PutMsg(req)
		if err != nil {
			panic(err)
		}
		time.Sleep(10 * time.Millisecond)
		numEnqueues++
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
		// taking snapshot before exiting
		logger.LogInfo("received kill signel. acquiring snapshot lock to take snapshot")
		worker.SnapshotLock.Lock()
		defer worker.SnapshotLock.UnLock()
		logger.LogInfo("taking final snapshot")
		worker.TakeSnapshot()
		logger.LogInfo("taken final snapshot. exiting")
	}
	logger.LogInfo("exiting !!!!!!")
	os.Exit(0)
}
