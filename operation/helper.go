package operation

import (
	"time"

	"github.com/ggvishnu29/horlix/model"

	"github.com/satori/go.uuid"
)

func BumpUpVersion(msg *model.Msg) {
	msg.Data.Version = time.Now().UnixNano()
}

func GenerateReceiptID() (string, error) {
	uuid := uuid.NewV4()
	return uuid.String(), nil
}
