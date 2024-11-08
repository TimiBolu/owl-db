package core

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"go.mongodb.org/mongo-driver/bson"
)

type Update map[string]interface{}

func applyUpdate(doc map[string]interface{}, update Update) error {
	for op, fields := range update {
		switch op {
		case "$set":
			setFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $set operation")
			}
			// Apply $set operation
			for key, value := range setFields {
				updateNestedField(doc, key, value)
			}

		case "$inc":
			incFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $inc operation")
			}
			// Apply $inc operation
			for key, value := range incFields {
				if err := incrementField(doc, key, value); err != nil {
					return err
				}
			}

		case "$unset":
			unsetFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $unset operation")
			}
			// Apply $unset operation
			for key := range unsetFields {
				deleteNestedField(doc, key)
			}

		case "$rename":
			renameFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $rename operation")
			}
			// Apply $rename operation
			for oldKey, newKey := range renameFields {
				if newKeyStr, ok := newKey.(string); ok {
					err := renameField(doc, oldKey, newKeyStr)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("new key in $rename must be a string")
				}
			}

		case "$push":
			pushFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $push operation")
			}
			// Apply $push operation
			for key, value := range pushFields {
				err := pushToArray(doc, key, value)
				if err != nil {
					return err
				}
			}

		case "$pull":
			pullFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $pull operation")
			}
			// Apply $pull operation
			for key, value := range pullFields {
				err := pullFromArray(doc, key, value)
				if err != nil {
					return err
				}
			}

		case "$pullAll":
			pullAllFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $pullAll operation")
			}
			// Apply $pullAll operation
			for key, values := range pullAllFields {
				if valueSlice, ok := values.([]interface{}); ok {
					for _, value := range valueSlice {
						err := pullFromArray(doc, key, value)
						if err != nil {
							return err
						}
					}
				} else {
					return fmt.Errorf("invalid value for $pullAll operation")
				}
			}

		case "$pop":
			popFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $pop operation")
			}
			// Apply $pop operation
			for key, value := range popFields {
				if v, ok := value.(float64); ok { // Pop value should be 1 or -1
					if v == 1 {
						err := popFromArray(doc, key, true)
						if err != nil {
							return err
						}
					} else if v == -1 {
						err := popFromArray(doc, key, false)
						if err != nil {
							return err
						}
					} else {
						return fmt.Errorf("invalid value for $pop operation")
					}
				} else {
					return fmt.Errorf("invalid value for $pop operation")
				}
			}

		case "$addToSet":
			addToSetFields, ok := fields.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid $addToSet operation")
			}
			// Apply $addToSet operation
			for key, value := range addToSetFields {
				err := addToSet(doc, key, value)
				if err != nil {
					return err
				}
			}

		default:
			return fmt.Errorf("unsupported update operator: %s", op)
		}
	}
	return nil
}

// renameField renames a field in a nested document.
func renameField(doc map[string]interface{}, oldKey string, newKey string) error {
	oldValue, exists := getNestedValue(doc, strings.Split(oldKey, "."))
	if !exists {
		return fmt.Errorf("field %s not found", oldKey)
	}
	deleteNestedField(doc, oldKey)
	updateNestedField(doc, newKey, oldValue)
	return nil
}

// pushToArray adds a value to an array at the specified path.
func pushToArray(doc map[string]interface{}, path string, value interface{}) error {
	keys := strings.Split(path, ".")
	lastKey := keys[len(keys)-1]

	// Traverse to the nested map where the last key belongs
	m := doc
	for _, key := range keys[:len(keys)-1] {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			// Create a new nested map if it doesn't exist
			newMap := make(map[string]interface{})
			m[key] = newMap
			m = newMap
		}
	}

	// Perform push operation
	if array, ok := m[lastKey].([]interface{}); ok {
		m[lastKey] = append(array, value)
	} else {
		m[lastKey] = []interface{}{value} // Create a new array if not exists
	}
	return nil
}

// pullFromArray removes a value from an array at the specified path.
func pullFromArray(doc map[string]interface{}, path string, value interface{}) error {
	keys := strings.Split(path, ".")
	lastKey := keys[len(keys)-1]

	// Traverse to the nested map where the last key belongs
	m := doc
	for _, key := range keys[:len(keys)-1] {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			// Field doesn't exist, nothing to pull
			return nil
		}
	}

	// Perform pull operation
	if array, ok := m[lastKey].([]interface{}); ok {
		for i, v := range array {
			if v == value { // Assuming equality check for simplicity
				m[lastKey] = append(array[:i], array[i+1:]...) // Remove the item
				break
			}
		}
	}
	return nil
}

// popFromArray removes the last or first element from an array at the specified path.
func popFromArray(doc map[string]interface{}, path string, fromEnd bool) error {
	keys := strings.Split(path, ".")
	lastKey := keys[len(keys)-1]

	// Traverse to the nested map where the last key belongs
	m := doc
	for _, key := range keys[:len(keys)-1] {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			return fmt.Errorf("field %s not found", path)
		}
	}

	// Perform pop operation
	if array, ok := m[lastKey].([]interface{}); ok {
		if len(array) == 0 {
			return nil // Nothing to pop
		}
		if fromEnd {
			m[lastKey] = array[:len(array)-1] // Remove the last item
		} else {
			m[lastKey] = array[1:] // Remove the first item
		}
	}
	return nil
}

// addToSet adds a value to an array at the specified path if it does not already exist.
func addToSet(doc map[string]interface{}, path string, value interface{}) error {
	keys := strings.Split(path, ".")
	lastKey := keys[len(keys)-1]

	// Traverse to the nested map where the last key belongs
	m := doc
	for _, key := range keys[:len(keys)-1] {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			// Create a new nested map if it doesn't exist
			newMap := make(map[string]interface{})
			m[key] = newMap
			m = newMap
		}
	}

	// Perform addToSet operation
	if array, ok := m[lastKey].([]interface{}); ok {
		// Check if the value already exists
		for _, v := range array {
			if v == value {
				return nil // Value already exists, do nothing
			}
		}
		m[lastKey] = append(array, value) // Add value to array
	} else {
		m[lastKey] = []interface{}{value} // Create new array if not exists
	}
	return nil
}

func updateNestedField(doc map[string]interface{}, path string, value interface{}) {
	keys := strings.Split(path, ".")
	lastKey := keys[len(keys)-1]

	// Traverse to the nested map where the last key belongs
	m := doc
	for _, key := range keys[:len(keys)-1] {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			// Create a new nested map if it doesn't exist
			newMap := make(map[string]interface{})
			m[key] = newMap
			m = newMap
		}
	}
	// Set the value
	m[lastKey] = value
}

func incrementField(doc map[string]interface{}, path string, increment interface{}) error {
	keys := strings.Split(path, ".")
	lastKey := keys[len(keys)-1]

	// Traverse to the nested map where the last key belongs
	m := doc
	for _, key := range keys[:len(keys)-1] {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			return fmt.Errorf("field %s not found", path)
		}
	}

	// Perform increment operation
	currentValue, ok := m[lastKey].(float64) // Assuming float64 for simplicity
	if !ok {
		return fmt.Errorf("field %s is not a number", path)
	}
	incValue, ok := increment.(float64)
	if !ok {
		return fmt.Errorf("invalid increment value")
	}

	m[lastKey] = currentValue + incValue
	return nil
}

func deleteNestedField(doc map[string]interface{}, path string) {
	keys := strings.Split(path, ".")
	lastKey := keys[len(keys)-1]

	// Traverse to the nested map where the last key belongs
	m := doc
	for _, key := range keys[:len(keys)-1] {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			// Field doesn't exist, nothing to unset
			return
		}
	}
	delete(m, lastKey)
}

func addIndexEntry(txn *badger.Txn, field string, value interface{}, docID string) error {
	indexKey := fmt.Sprintf("%s|%v|%s", field, value, docID)
	return txn.Set([]byte(indexKey), []byte(docID))
}

func removeIndexEntry(txn *badger.Txn, field string, value interface{}, docID string) error {
	indexKey := fmt.Sprintf("%s|%v|%s", field, value, docID)
	return txn.Delete([]byte(indexKey))
}

// Example usage of an update operation
//
//	update := Update{
//		"$set": map[string]interface{}{
//			"price":     1500,
//			"address.street": "New Street Name",
//		},
//		"$inc": map[string]interface{}{
//			"ratings.score": 1,
//		},
//	}
func (c *Collection[T]) UpdateByID(docID string, update Update) error {
	return c.Db.Update(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s|%s", c.Name, docID)

		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		var doc map[string]interface{}
		err = item.Value(func(val []byte) error {
			return bson.Unmarshal(val, &doc)
		})
		if err != nil {
			return err
		}

		return nativeUpdate(c, txn, doc, docID, update)
	})
}

func (c *Collection[T]) UpdateOne(filter Filter, update Update) error {
	return c.Db.Update(func(txn *badger.Txn) error {
		result := nativeFindOne(c, txn, filter)
		if !result.found {
			return errors.New("no document found in result")
		}

		doc := result.doc
		docID := result.doc["_id"].(string)

		return nativeUpdate(c, txn, doc, docID, update)
	})
}

func (c *Collection[T]) UpdateMany(filter Filter, update Update) error {
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

					// Update the document within the same transaction
					err := nativeUpdate(c, txn, doc, docID, update)
					if err != nil {
						errCh <- fmt.Errorf("failed to update doc %s: %v", docID, err)
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
