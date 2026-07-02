package main

import (
	"flag"
	"log"
	"net/http"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	s := store.NewMemStore(clock.Real{})
	log.Printf("quorum-store listening on %s", *addr)
	if err := http.ListenAndServe(*addr, api.NewServer(s)); err != nil {
		log.Fatal(err)
	}
}
