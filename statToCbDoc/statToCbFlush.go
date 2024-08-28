package main

import (
	"log"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbFlush:init()")
}

func statToCbFlush() {
	log.Println("statToCbFlush()")

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
}
