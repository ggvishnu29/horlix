package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
	"github.com/ggvishnu29/horlix/worker"
)

func main() {
	logger.Init()
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
	for true {
		msg, err := operation.GetMsg("tube1")
		if err != nil {
			panic(err)
		}
		if msg == nil {
			logger.LogInfo("no msg in the queue")
			continue
		}
		b, _ := json.Marshal(msg)
		logger.LogInfof("msg: %v\n", string(b))
		operation.DeleteMsg(msg.Tube.ID, msg.ID)
		//time.Sleep(1 * time.Second)
	}
}

func enqueueMsgs() {
	for true {
		err := operation.PutMsg("tube1", "msg1", []byte("hello"), 1, 0)
		if err != nil {
			panic(err)
		}
		err = operation.PutMsg("tube1", "msg2", []byte("world"), 1, 0)
		if err != nil {
			panic(err)
		}
		err = operation.PutMsg("tube1", "msg1", []byte("world"), 1, 0)
		if err != nil {
			panic(err)
		}
		//time.Sleep(3 * time.Second)
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
	os.Exit(0)
}
