package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ggvishnu29/horlix/contract"
	"github.com/ggvishnu29/horlix/model"
	"github.com/ggvishnu29/horlix/operation"

	"github.com/ggvishnu29/horlix/logger"
)

func RecoverFromTransLog() error {
	if err := loadDataFromSnapshot(); err != nil {
		return err
	}
	for _, tube := range model.TMap.Tubes {
		operation.SpawnTubeWorkersChan <- tube
	}
	info, err := os.Stat(transLogPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.LogInfo("no transaction log found. nothing to recover")
			return nil
		}
		return err
	}
	if info.Size() == 0 {
		logger.LogInfo("transaction log size is zero. nothing to recover")
		return nil
	}
	tFile, err := os.Open(transLogPath)
	if err != nil {
		return err
	}
	defer tFile.Close()
	logger.SetTransLogRecoveryFlag()
	defer logger.UnSetTransLogRecoveryFlag()
	scanner := bufio.NewScanner(tFile)
	tCount := 0
	for scanner.Scan() {
		opr := scanner.Text()
		if scanner.Scan() != true {
			if err := scanner.Err(); err != nil {
				return err
			}
			return fmt.Errorf("data corruption in trans log. req data missing in trans log")
		}
		reqString := scanner.Text()
		switch opr {
		case operation.PutMsgOpr:
			req := &contract.PutMsgRequest{}
			if err := json.Unmarshal([]byte(reqString), req); err != nil {
				return err
			}
			if err := operation.PutMsg(req); err != nil {
				return err
			}
		case operation.GetMsgOpr:
			req := &contract.GetMsgRequest{}
			if err := json.Unmarshal([]byte(reqString), req); err != nil {
				return err
			}
			if _, err := operation.GetMsg(req); err != nil {
				return err
			}
		case operation.ReleaseMsgOpr:
			req := &contract.ReleaseMsgRequest{}
			if err := json.Unmarshal([]byte(reqString), req); err != nil {
				return err
			}
			if err := operation.ReleaseMsg(req); err != nil {
				return err
			}
		case operation.AckMsgOpr:
			req := &contract.AckMsgRequest{}
			if err := json.Unmarshal([]byte(reqString), req); err != nil {
				return err
			}
			if err := operation.AckMsg(req); err != nil {
				return err
			}
		case operation.DeleteMsgOpr:
			req := &contract.DeleteMsgRequest{}
			if err := json.Unmarshal([]byte(reqString), req); err != nil {
				return err
			}
			if err := operation.DeleteMsg(req); err != nil {
				return err
			}
		case operation.CreateTubeOpr:
			req := &contract.CreateTubeRequest{}
			if err := json.Unmarshal([]byte(reqString), req); err != nil {
				return err
			}
			if err := operation.CreateTube(req); err != nil {
				return err
			}
		case operation.DeleteTubeOpr:
			req := &contract.DeleteTubeRequest{}
			if err := json.Unmarshal([]byte(reqString), req); err != nil {
				return err
			}
			if err := operation.DeleteTube(req); err != nil {
				return err
			}
		default:
			panic("unknown operation found in trans log: " + opr)
		}
		tCount++
	}
	logger.LogInfof("recovered %v operations from trans log\n", tCount)
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func loadDataFromSnapshot() error {
	file, err := os.Open(snapshotFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.LogInfo("no snapshot exists. So, starting fresh..")
			return nil
		}
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	byteString, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	bytes := []byte(byteString)
	tmap := &model.TubeMap{}
	if err := json.Unmarshal(bytes, tmap); err != nil {
		return err
	}
	model.TMap = tmap
	//fmt.Println(line)
	// scanner := bufio.NewScanner(file)
	// var byteString string
	// for scanner.Scan() {
	// 	byteString = scanner.Text()
	// }
	// if err := scanner.Err(); err != nil {
	// 	return err
	// }
	// fmt.Println(byteString)
	// bytes := []byte(byteString)

	// tmap := &model.TubeMap{}
	// if err := json.Unmarshal(bytes, tmap); err != nil {
	// 	return err
	// }
	// model.TMap = tmap
	return nil
}
