package logger

import (
	"testing"
)

func Test_InitAppLogger(t *testing.T) {
	err := InitAppLogger("")
	if err != nil {
		t.Errorf("expected log horlix.log in current directory, got %v", err)
	}
}
