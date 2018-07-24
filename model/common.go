package model

import "github.com/ggvishnu29/horlix/serde"

var LogWorkerChan chan *serde.Operation = make(chan *serde.Operation, 100000)

const DELAYED_QUEUE = "delayed_queue"
const READY_QUEUE = "ready_queue"
const RESERVED_QUEUE = "reserved_queue"
const MSG_MAP = "msg_map"
const TUBE_MAP = "tube_map"
const MSG = "msg"
const TUBE = "tube"
const ENQUEUE_OPR = "enqueue"
const DEQUEUE_OPR = "dequeue"
const ADD_OR_UPDATE_OPR = "addorupdate"
const DELETE_OPR = "delete"
const PUT_OPR = "put"
const SET_MSG_STATE_OPR = "setmsgstate"
const SET_RESERVED_TIMESTAMP_OPR = "setreservedtime"
const SET_DELAYED_TIMESTAMP_OPR = "setdelatedtime"
const SET_FIRST_ENQUEUED_TIMESTAMP_OPR = "setfirstenqueuedtime"
const SET_RECEIPT_ID_OPR = "setreceiptid"
const SET_DATA_OPR = "setdata"
const SET_MSG_DELETED_OPR = "setmsgdeleted"
const SET_WAITING_DATA_OPR = "setwaitingdata"
const SET_TUBE_DELETED_OPR = "settubedeleted"
const SET_DATA_SLICE_OPR = "setdataslice"
const SET_WAITING_DATA_SLICE_OPR = "setwaitingdataslice"
const APPEND_DATA_SLICE_OPR = "appenddataslice"
const APPEND_WAITING_DATA_SLICE_OPR = "appenwaitingdataslice"
const APPEND_WAITING_DATA_TO_DATA_SLICE_OPR = "appendwaitingdatatodataslice"
const REPLACE_DATA_WITH_WAITING_DATA_SLICE_OPR = "replacedatawithwaitingdatasliceopr"
const MOVE_WAITING_DATA_TO_DATA = "movewaitingdatatodata"

func LogOpr(opr *serde.Operation) {
	LogWorkerChan <- opr
}
