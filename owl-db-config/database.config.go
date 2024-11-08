package config

import (
	"log"

	badger "github.com/dgraph-io/badger/v4"
)

var BadgerDBClient *badger.DB

func ConnectBadgerDB() {
	opts := badger.DefaultOptions("./badger")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	BadgerDBClient = db
}

func DisconnectBadgerDB() {
	err := BadgerDBClient.Close()
	if err != nil {
		panic(err)
	}
}
