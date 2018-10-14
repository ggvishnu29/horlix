package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/ggvishnu29/horlix/contract"
	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"
	"github.com/ggvishnu29/horlix/serde"
	"github.com/ggvishnu29/horlix/worker"
)

func TestMain(m *testing.M) {
	dir, err := ioutil.TempDir("/tmp", "horlix-unit-testing")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	logger.InitAppLogger(dir)
	logger.InitTransLogger(dir)
	serde := &serde.JSONSerde{}
	logWorker := worker.NewTransLogWorker(serde)
	go logWorker.StartTransLogWorker()
	go worker.StartTubesManager()
	fmt.Println("initialized horlix")
	os.Exit(m.Run())
}

func TestCreateTube(t *testing.T) {
	req := &contract.CreateTubeRequest{
		TubeName:            "tube1",
		ReserveTimeoutInSec: 10,
		DataFuseSetting:     1,
	}
	if err := operation.CreateTube(req); err != nil {
		t.Fatal(err)
	}
	tube := model.GetTubeMap().GetTube("tube1")
	if tube == nil {
		t.Fatal("tube is not initialized properly")
	}
	if tube.ID != "tube1" {
		t.Fatal("tube id is not initialized properly")
	}
	if tube.ReserveTimeoutInSec != 10 {
		t.Fatal("reserve timeout in sec config is not initialized properly")
	}
	if tube.FuseSetting.Data != 1 {
		t.Fatal("data fuse setting is not initialized properly")
	}
	if err := operation.CreateTube(req); err == nil {
		t.Fatal("expected error while creating tube with duplicate tube id")
	}
}

func TestDeleteTube(t *testing.T) {
	cReq := &contract.CreateTubeRequest{
		TubeName:            "tube3",
		ReserveTimeoutInSec: 10,
		DataFuseSetting:     1,
	}
	if err := operation.CreateTube(cReq); err != nil {
		t.Fatal(err)
	}
	tube := model.GetTubeMap().GetTube("tube3")
	dReq := &contract.DeleteTubeRequest{
		TubeName: "tube3",
	}
	if err := operation.DeleteTube(dReq); err != nil {
		t.Fatal(err)
	}
	if tube.IsDeleted != true {
		t.Fatal("IsDeleted flag is not set to true for a deleted tube")
	}
	if err := operation.DeleteTube(dReq); err == nil {
		t.Fatal("expected error while deleting a non-existent tube")
	}
}

func TestPutGetAckMsg(t *testing.T) {
	cReq := &contract.CreateTubeRequest{
		TubeName:            "tube4",
		ReserveTimeoutInSec: 10,
		DataFuseSetting:     1,
	}
	if err := operation.CreateTube(cReq); err != nil {
		t.Fatal(err)
	}
	pReq := &contract.PutMsgRequest{
		MsgID:      "msg1",
		TubeID:     "tube4",
		DataBytes:  []byte("test1"),
		DelayInSec: 0,
		Priority:   1,
	}
	if err := operation.PutMsg(pReq); err != nil {
		t.Fatal(err)
	}
	gReq := &contract.GetMsgRequest{
		TubeID: "tube4",
	}
	msg, err := operation.GetMsg(gReq)
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != "msg1" {
		t.Fatal("msg id is different than expected")
	}
	if !reflect.DeepEqual(msg.Data.DataSlice[0], pReq.DataBytes) {
		t.Fatal("msg data is different than expected")
	}
	if msg.TubeName != "tube4" {
		t.Fatal("tube name is different than expected")
	}
	if msg.ReceiptID == nil {
		t.Fatal("receipt id is not set")
	}
	aReq := &contract.AckMsgRequest{
		MsgID:     "msg1",
		TubeID:    "tube4",
		ReceiptID: msg.ReceiptID,
	}
	if err := operation.AckMsg(aReq); err != nil {
		t.Fatal(err)
	}
	if msg.IsDeleted != true {
		t.Fatal("msg is not deleted as expected")
	}
}

func TestPutGetReleaseMsg(t *testing.T) {
	cReq := &contract.CreateTubeRequest{
		TubeName:            "tube5",
		ReserveTimeoutInSec: 10,
		DataFuseSetting:     1,
	}
	if err := operation.CreateTube(cReq); err != nil {
		t.Fatal(err)
	}
	pReq := &contract.PutMsgRequest{
		MsgID:      "msg1",
		TubeID:     "tube5",
		DataBytes:  []byte("test1"),
		DelayInSec: 0,
		Priority:   1,
	}
	if err := operation.PutMsg(pReq); err != nil {
		t.Fatal(err)
	}
	gReq := &contract.GetMsgRequest{
		TubeID: "tube5",
	}
	msg, err := operation.GetMsg(gReq)
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != "msg1" {
		t.Fatal("msg id is different than expected")
	}
	if !reflect.DeepEqual(msg.Data.DataSlice[0], pReq.DataBytes) {
		t.Fatal("msg data is different than expected")
	}
	if msg.TubeName != "tube5" {
		t.Fatal("tube name is different than expected")
	}
	if msg.ReceiptID == nil {
		t.Fatal("receipt id is not set")
	}
	rReq := &contract.ReleaseMsgRequest{
		TubeID:    "tube5",
		MsgID:     "msg1",
		ReceiptID: msg.ReceiptID,
	}
	if err := operation.ReleaseMsg(rReq); err != nil {
		t.Fatal(err)
	}
}
