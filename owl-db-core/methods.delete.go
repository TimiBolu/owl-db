package core

import (
	"errors"
	"fmt"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

func (c *Collection[T]) DeleteByID(docID string) error {
	return c.Db.Update(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s|%s", c.Name, docID)

		_, err := txn.Get([]byte(key))
		if err != nil {
			return err // Document not found
		}

		// Delete document from the collection
		err = txn.Delete([]byte(key))
		if err != nil {
			return err
		}

		// Optionally handle index removal if needed
		// (If you manage indexes similarly as with updates)

		return nil
	})
}

func (c *Collection[T]) DeleteOne(filter Filter) error {
	return c.Db.Update(func(txn *badger.Txn) error {
		result := nativeFindOne(c, txn, filter)
		if !result.found {
			return errors.New("no document found in result")
		}

		docID := result.doc["_id"].(string)
		key := fmt.Sprintf("%s|%s", c.Name, docID)

		// Delete the document from the collection
		err := txn.Delete([]byte(key))
		if err != nil {
			return err
		}

		// Optionally handle index removal if needed
		// (If you manage indexes similarly as with updates)

		return nil
	})
}

func (c *Collection[T]) DeleteMany(filter Filter) error {
	return c.Db.Update(func(txn *badger.Txn) error {
		results := nativeFind(c, txn, filter)

		if len(results) == 0 {
			return nil // No matching documents
		}

		// Create channels to handle concurrency
		docCh := make(chan map[string]interface{}, len(results)) // Channel to send docs to worker goroutines
		errCh := make(chan error, len(results))                  // Channel to capture errors
		workerCount := 5                                         // Number of concurrent workers
		var wg sync.WaitGroup                                    // WaitGroup for tracking completion

		// Start worker goroutines
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for doc := range docCh {
					docID := doc["_id"].(string)
					key := fmt.Sprintf("%s|%s", c.Name, docID)

					// Use the passed-in transaction `txn` to delete the document
					err := txn.Delete([]byte(key))
					if err != nil {
						errCh <- fmt.Errorf("failed to delete doc %s: %v", docID, err)
					}
				}
			}()
		}

		// Send documents to workers
		for _, doc := range results {
			docCh <- doc
		}

		close(docCh) // Close the doc channel when all documents are sent

		// Wait for all workers to finish
		wg.Wait()

		close(errCh) // Close the error channel

		// Collect and return any errors
		var finalErr error
		for err := range errCh {
			if finalErr == nil {
				finalErr = err
			} else {
				finalErr = fmt.Errorf("%v; %v", finalErr, err)
			}
		}

		return finalErr
	})
}
