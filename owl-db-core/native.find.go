package core

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func nativeFind[T Document](
	c *Collection[T],
	txn *badger.Txn,
	filter Filter,
	findOptions ...FindOptions,
) []map[string]interface{} {
	var options FindOptions
	if (len(findOptions)) > 0 {
		options = findOptions[0]
	}
	var results []map[string]interface{}
	var mu sync.Mutex
	processedDocs := make(map[string]struct{}) // To track processed document keys

	// Initialize iterator
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	// Scan through all documents in the collection
	prefix := []byte(c.Name + "|")
	batchSize := 100 // Size of each batch for parallel processing
	batch := make([]*badger.Item, 0, batchSize)

	// Completion channel to signal when all batches are processed
	completionCh := make(chan struct{})

	// Channel to process batches in parallel
	batchCh := make(chan []*badger.Item)

	// Goroutine to process batches
	go func() {
		for batch := range batchCh {
			localResults := []map[string]interface{}{}
			for _, item := range batch {
				err := item.Value(func(val []byte) error {
					var doc map[string]interface{}
					if err := bson.Unmarshal(val, &doc); err != nil {
						fmt.Printf("Deserialization error: %v\n", err)
						return err
					}

					// Check if this document has already been processed
					docKey := string(item.Key())
					mu.Lock()
					if _, found := processedDocs[docKey]; found {
						mu.Unlock()
						return nil // Skip if the document was already processed
					}
					processedDocs[docKey] = struct{}{} // Mark this document as processed
					mu.Unlock()

					// Apply filters to the document
					// Main function to apply filters and selections
					if matchDocument(doc, filter) {
						// Apply selection
						selectedDoc := make(map[string]interface{})

						// Copy the original document to selectedDoc
						for key, value := range doc {
							selectedDoc[key] = value
						}

						// Recursively remove excluded fields from the selected document
						removeExcludedFields(selectedDoc, options.Select, "")

						// Only add to localResults if selectedDoc is not empty
						if len(selectedDoc) > 0 {
							localResults = append(localResults, selectedDoc)
						}
					}
					return nil
				})

				if err != nil {
					fmt.Printf("Error processing item: %v\n", err)
					continue
				}
			}

			// Safely append to global results
			mu.Lock()
			results = append(results, localResults...)
			mu.Unlock()
		}
		// Signal completion when done processing all batches
		close(completionCh)
	}()

	// Iterate over the documents
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		batch = append(batch, item)

		// When batch size is reached, send it for processing
		if len(batch) == batchSize {
			batchCopy := make([]*badger.Item, len(batch))
			copy(batchCopy, batch)
			batchCh <- batchCopy
			batch = make([]*badger.Item, 0, batchSize) // Reset batch
		}
	}

	// Process the remaining documents if any
	if len(batch) > 0 {
		batchCopy := make([]*badger.Item, len(batch))
		copy(batchCopy, batch)
		batchCh <- batchCopy
	}

	// Close the batch channel to indicate no more batches
	close(batchCh)

	// Wait for all batches to be processed
	<-completionCh

	// Implement sorting
	if len(options.Sort) > 0 {
		sort.Slice(results, func(i, j int) bool {
			for _, sortField := range options.Sort {
				// split the sortedField and recursively determine the subfield
				// somemap["a.b.c"] --> invalid in go

				var valI, valJ interface{}
				var okI, okJ bool
				keys := strings.Split(sortField.Field, ".")
				if len(keys) > 0 {
					valI, okI = getNestedValue(results[i], keys)
					valJ, okJ = getNestedValue(results[j], keys)
				} else {
					valI, okI = results[i][sortField.Field]
					valJ, okJ = results[j][sortField.Field]
				}

				// Handle nil values
				if !okI && !okJ {
					continue // Both values are nil, continue to next sort field
				}
				if !okI {
					return sortField.Order == 1 // If valI is nil, it comes last in ascending order
				}
				if !okJ {
					return sortField.Order != 1 // If valJ is nil, it comes last in descending order
				}

				// Type assertion and comparison
				switch vI := valI.(type) {
				case string:
					vJ := valJ.(string)
					if sortField.Order == 1 {
						return vI < vJ // Ascending
					}
					return vI > vJ // Descending
				case int, int64:
					vJ := valJ.(int64) // Treat all integer types as int64 for comparison
					if sortField.Order == 1 {
						return vI.(int64) < vJ
					}
					return vI.(int64) > vJ
				case float32, float64:
					vJ := valJ.(float64) // Treat all floats types as float64 for comparison
					if sortField.Order == 1 {
						return vI.(float64) < vJ
					}
					return vI.(float64) > vJ
				case primitive.DateTime:
					vJ := valJ.(primitive.DateTime)
					if sortField.Order == 1 {
						return vI < vJ
					}
					return vI > vJ
				case time.Time:
					vJ := valJ.(time.Time)
					fmt.Println(vI, vJ)
					if sortField.Order == 1 {
						return vI.Before(vJ) // Ascending
					}
					return vI.After(vJ) || vI.Equal(vJ) // Descending
				// Add more cases here for different types if needed
				default:
					// If types don't match or are unsupported, don't sort them
					fmt.Println("types don't match")

					return false
				}
			}
			return false // Default case, no order change
		})
	}

	// Apply skip and limit
	if options.Skip > 0 {
		if options.Skip >= len(results) {
			return []map[string]interface{}{} // If skip is greater than results, return empty
		}
		results = results[options.Skip:]
	}

	if options.Limit > 0 && options.Limit < len(results) {
		results = results[:options.Limit] // Limit results
	}

	return results
}

func nativeFindOne[T Document](
	c *Collection[T],
	txn *badger.Txn,
	filter Filter,
) FoundDocStruct {
	var foundDoc = make(chan FoundDocStruct, 1) // Channel to signal found document
	var stopSearch = make(chan struct{})        // Channel to signal other goroutines to stop

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	prefix := []byte(c.Name + "|")
	batchSize := 100 // Size of each batch for parallel processing
	batch := make([]*badger.Item, 0, batchSize)

	var wg sync.WaitGroup
	var mu sync.Mutex

	// Function to process a batch of documents
	processBatch := func(batch []*badger.Item) {
		for _, item := range batch {
			select {
			case <-stopSearch:
				// Stop processing if another goroutine found the document
				return
			default:
			}

			err := item.Value(func(val []byte) error {
				var doc map[string]interface{}
				if err := bson.Unmarshal(val, &doc); err != nil {
					fmt.Printf("Deserialization error: %v\n", err)
					return err
				}

				// Apply filters to the document
				if matchDocument(doc, filter) {
					mu.Lock()
					foundDoc <- FoundDocStruct{doc: doc, found: true}
					mu.Unlock()
				}

				return nil
			})

			if err != nil {
				fmt.Printf("Error processing item: %v\n", err)
				continue
			}
		}
	}

	// Iterate over the collection
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		batch = append(batch, item)

		// Process batch when batch size is reached
		if len(batch) == batchSize {
			wg.Add(1)
			go func() {
				defer wg.Done()
				processBatch(batch)
			}()
			batch = make([]*badger.Item, 0, batchSize) // Reset batch
		}
	}

	// Process remaining documents in the batch
	if len(batch) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processBatch(batch)
		}()
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(foundDoc)
	}()

	// Listen for found document or completion
	result := <-foundDoc
	close(stopSearch)

	return result
}
