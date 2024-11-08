package core

import (
	"fmt"
	"strings"

	utils "github.com/TimiBolu/owl-db/owl-db-utils"
	badger "github.com/dgraph-io/badger/v4"
	"go.mongodb.org/mongo-driver/bson"
)

type Collection[T Document] struct {
	Db         *badger.DB
	Name       string
	Indexes    []string
	Timestamp  bool
	EdgeLabels []string
	edgePrefix string
}

// Collection options and configuration
type CollectionOptions struct {
	Timestamp bool
	Indexes   []string
	// Edge specific options
	EdgeLabels []string
}

// Key prefixes for different types of data
const (
	nodePrefix = "n:" // Node prefix
	edgePrefix = "e:" // Edge prefix
	idxPrefix  = "i:" // Index prefix
)

func NewCollection[T Document](db *badger.DB, name string, opts ...CollectionOptions) *Collection[T] {
	var timestamp bool
	var indexes, edgeLabels []string

	if len(opts) > 0 {
		timestamp = opts[0].Timestamp
		indexes = utils.RemoveDuplicates(opts[0].Indexes)
		edgeLabels = utils.RemoveDuplicates(opts[0].EdgeLabels)
	}

	return &Collection[T]{
		Db:         db,
		Name:       name,
		Indexes:    indexes,
		Timestamp:  timestamp,
		EdgeLabels: edgeLabels,
		edgePrefix: fmt.Sprintf("%s%s:", edgePrefix, name),
	}
}

func getIndexableFields(doc interface{}, indexFields []string) map[string]interface{} {
	indexableFields := make(map[string]interface{})

	// Marshal the document back to BSON to inspect fields (if necessary)
	bsonDoc, err := bson.Marshal(doc)
	if err != nil {
		// Handle error if necessary
		return indexableFields
	}

	// Unmarshal into a map to extract values
	var docMap map[string]interface{}
	if err := bson.Unmarshal(bsonDoc, &docMap); err != nil {
		// Handle error if necessary
		return indexableFields
	}

	for _, field := range indexFields {
		// Split the field into parts based on dot notation
		parts := strings.Split(field, ".")
		value, exists := getNestedValue(docMap, parts)

		if exists {
			indexableFields[field] = value
		}
	}
	return indexableFields
}

// getNestedValue retrieves the value from a BSON map given a list of keys (parts).
func getNestedValue(docMap map[string]interface{}, keys []string) (interface{}, bool) {
	var value interface{} = docMap

	for _, key := range keys {
		if nestedDoc, ok := value.(map[string]interface{}); ok {
			if val, exists := nestedDoc[key]; exists {
				value = val
			} else {
				return nil, false // Key does not exist
			}
		} else {
			return nil, false // Not a Document, cannot traverse further
		}
	}
	return value, true // Successfully found the value
}
