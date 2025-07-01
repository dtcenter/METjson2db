package main

import (
	"log/slog"
	"os"
	"testing"

	"github.com/dtcenter/METjson2db/pkg/core"
)

func TestParseLoadSpec(t *testing.T) {
	slog.Info("TestParseLoadSpec")

	dir, err := os.Getwd()
	if err != nil {
		t.Errorf("TestParseLoadSpec error:%v", err)
		return
	}
	_, err = core.ParseLoadSpec(dir + "/../../load_spec.json")
	if err != nil {
		t.Errorf("TestParseLoadSpec error:%v", err)
		return
	}
}
