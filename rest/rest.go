package rest

import "github.com/labstack/echo"

var e *echo.Echo

const INVALID_REQUEST_ERR_CODE = 422
const INTERNAL_SERVER_ERR_CODE = 500
const CONFLICT_ERR_CODE = 409

func Init() {
	e := echo.New()
	e.POST("/tube", createTube)
	panic(e.Start(":1234"))
}
