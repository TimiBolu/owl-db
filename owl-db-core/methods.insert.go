package core

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (c *Collection[T]) Insert(doc T) error {
	// Check if the document already has an ID
	if doc.GetID() == "" {
		// Generate a unique document ID and set it
		doc.SetID(primitive.NewObjectID().Hex())
	}

	return c.Db.Update(func(txn *badger.Txn) error {
		docID := doc.GetID()
		key := fmt.Sprintf("%s|%s", c.Name, docID)

		if c.Timestamp {
			doc.SetCreatedAt()
		}

		// Serialize the document
		serializedDoc, err := bson.Marshal(doc)
		if err != nil {
			return err
		}

		// Store the document in Badger
		if err := txn.Set([]byte(key), serializedDoc); err != nil {
			return err
		}

		// Get the indexable fields
		indexableFields := getIndexableFields(doc, c.Indexes)

		// Update indexes for the indexable fields
		for field, value := range indexableFields {
			indexKey := fmt.Sprintf("%s|%s|%v|%s", c.Name, field, value, docID)
			if err := txn.Set([]byte(indexKey), []byte(docID)); err != nil {
				return err
			}
		}

		return nil
	})
}

func (c *Collection[T]) InsertMany(docs []T) error {
	// Function to insert a single document in the transaction
	insertDoc := func(txn *badger.Txn, doc T) error {
		// Check if the document already has an ID
		if doc.GetID() == "" {
			doc.SetID(primitive.NewObjectID().Hex())
		}

		// Generate a unique document ID
		docID := doc.GetID()
		key := fmt.Sprintf("%s|%s", c.Name, docID)

		// Serialize the document
		serializedDoc, err := bson.Marshal(doc)
		if err != nil {
			return err
		}

		// Store the document in Badger
		if err := txn.Set([]byte(key), serializedDoc); err != nil {
			return err
		}

		// Get the indexable fields
		indexableFields := getIndexableFields(doc, c.Indexes)

		// Update indexes for the indexable fields
		for field, value := range indexableFields {
			indexKey := fmt.Sprintf("%s|%s|%v|%s", c.Name, field, value, docID)
			if err := txn.Set([]byte(indexKey), []byte(docID)); err != nil {
				return err
			}
		}

		return nil
	}

	// Perform the batch insert operation in a single transaction
	err := c.Db.Update(func(txn *badger.Txn) error {
		for _, doc := range docs {
			if err := insertDoc(txn, doc); err != nil {
				return err
			}
		}

		return nil
	})

	return err
}
