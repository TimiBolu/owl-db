package core

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"go.mongodb.org/mongo-driver/bson"
)

func nativeUpdate[T Document](
	c *Collection[T],
	txn *badger.Txn,
	doc map[string]interface{},
	docID string,
	update Update,
) error {

	oldIndexableFields := getIndexableFields(doc, c.Indexes)

	// Apply the update (assume `applyUpdate` function handles it correctly)
	err := applyUpdate(doc, update)
	if err != nil {
		return err
	}

	newIndexableFields := getIndexableFields(doc, c.Indexes)

	// Handle index updates
	for field, oldValue := range oldIndexableFields {
		newValue, exists := newIndexableFields[field]
		if !exists || newValue != oldValue {
			// Remove old index entry
			err := removeIndexEntry(txn, field, oldValue, docID)
			if err != nil {
				return err
			}
		}
	}

	for field, newValue := range newIndexableFields {
		oldValue, exists := oldIndexableFields[field]
		if !exists || newValue != oldValue {
			// Add new index entry
			err := addIndexEntry(txn, field, newValue, docID)
			if err != nil {
				return err
			}
		}
	}

	if c.Timestamp {
		doc["updatedAt"] = time.Now()
	}

	// Serialize and write back the updated document
	updatedData, err := bson.Marshal(doc)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s|%s", c.Name, docID)
	err = txn.Set([]byte(key), updatedData)
	if err != nil {
		return err
	}

	return nil
}
