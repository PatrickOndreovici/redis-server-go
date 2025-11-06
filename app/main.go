package main

import (
	"github.com/codecrafters-io/redis-starter-go/app/pkg"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/store"
)

func main() {
	inMemoryStore := &store.Store{KV: store.NewKVStore(),
		Lists: store.NewListsStore(), StreamStore: store.NewStreamStore()}
	server := pkg.NewServer("0.0.0.0:6379", inMemoryStore)
	err := server.ListenAndServe()
	if err != nil {
		return
	}
}
