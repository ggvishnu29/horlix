package rest

import (
	"strconv"

	"github.com/ggvishnu29/horlix/contract"
	"github.com/ggvishnu29/horlix/logger"
	"github.com/ggvishnu29/horlix/operation"
	"github.com/labstack/echo"
)

func createTube(c echo.Context) error {
	tubeName := c.FormValue("tube_name")
	rTimeoutStr := c.FormValue("reserve_timeout_in_sec")
	dataFuseStr := c.FormValue("data_fuse_option")
	rTimeoutInSec, err := strconv.ParseInt(rTimeoutStr, 10, 32)
	if err != nil {
		return echo.NewHTTPError(INVALID_REQUEST_ERR_CODE,
			"unable to parse reserve_timeout_in_sec param")
	}
	if rTimeoutInSec <= 0 {
		return echo.NewHTTPError(INVALID_REQUEST_ERR_CODE,
			"reserve_timeout_in_sec should be greater than zero")
	}
	dataFuseSetting, err := strconv.Atoi(dataFuseStr)
	if err != nil {
		return echo.NewHTTPError(INVALID_REQUEST_ERR_CODE,
			"unable to parse data_fuse_option")
	}
	if dataFuseSetting < 0 && dataFuseSetting > 1 {
		return echo.NewHTTPError(INVALID_REQUEST_ERR_CODE,
			"data_fuse_option should be either 0 or 1")
	}
	gReq, err := contract.NewGetTubeRequest(tubeName)
	if err != nil {
		return echo.NewHTTPError(INVALID_REQUEST_ERR_CODE, err.Error())
	}
	tube, err := operation.GetTube(gReq)
	if err != nil {
		logger.LogInfo(err.Error())
		return echo.NewHTTPError(INTERNAL_SERVER_ERR_CODE, "internal server error")
	}
	if tube != nil {
		return echo.NewHTTPError(CONFLICT_ERR_CODE, "tube already exists")
	}
	cReq, err := contract.NewCreateTubeRequest(tubeName, rTimeoutInSec, dataFuseSetting)
	if err != nil {
		return echo.NewHTTPError(INVALID_REQUEST_ERR_CODE, err.Error())
	}
	if err := operation.CreateTube(cReq); err != nil {
		logger.LogInfof("error while creating tube: %v", err.Error())
		return echo.NewHTTPError(INTERNAL_SERVER_ERR_CODE, "internal server error")
	}
	return nil
}
