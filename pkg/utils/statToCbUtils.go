package utils

import (
	"log"
	"log/slog"

	"github.com/relvacode/iso8601"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	slog.Debug("StatToCbUtils:init()")
}

func StatDateToEpoh(dateStr string) int64 {
	// 20240203_120000 => 2024-02-03T12:00:00
	yyyy := dateStr[0:4]
	mm := dateStr[4:6]
	dd := dateStr[6:8]
	hh := dateStr[9:11]
	strISO8601 := yyyy + "-" + mm + "-" + dd + "T" + hh + ":00:00"
	// log.Printf("strISO8601:", strISO8601)
	t, _ := iso8601.ParseString(strISO8601)

	return int64(t.Unix())
}
