package core

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"go.mongodb.org/mongo-driver/bson"
)

// Filter represents a query filter
type Filter map[string]interface{}

type FindOptions struct {
	Skip   int             // Number of documents to skip
	Limit  int             // Maximum number of documents to return
	Sort   []SortField     // Fields to sort by
	Select map[string]bool // Fields to select (if true, include; if false, exclude)
}

type SortField struct {
	Field string
	Order int // 1 for ascending, -1 for descending
}

// Find By Id is a special case of find. It's a simple "txn.Get" op in a collection
func (c *Collection[T]) FindByID(docID string) (map[string]interface{}, error) {
	var result map[string]interface{}

	err := c.Db.View(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s|%s", c.Name, docID)
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			err := bson.Unmarshal(val, &result)
			if err != nil {
				fmt.Printf("Deserialization error: %v\n", err)
			}
			return err
		})
		return err
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

type FoundDocStruct struct {
	doc   map[string]interface{}
	found bool
}

func (c *Collection[T]) FindOne(filter Filter) (map[string]interface{}, error) {
	var result FoundDocStruct

	err := c.Db.View(func(txn *badger.Txn) error {
		result = nativeFindOne(c, txn, filter)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result.doc, nil
}

func (c *Collection[T]) Find(filter Filter, findOptions ...FindOptions) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	var options FindOptions
	if (len(findOptions)) > 0 {
		options = findOptions[0]
	}

	err := c.Db.View(func(txn *badger.Txn) error {
		results = nativeFind(c, txn, filter, options)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

// matchDocument checks if a document matches the provided filter
func matchDocument(doc map[string]interface{}, filters Filter) bool {
	for field, value := range filters {
		switch field {
		case "$and":
			subFilters, ok := value.([]Filter)
			if !ok {
				return false
			}
			// All subfilters must match
			for _, subFilter := range subFilters {
				if !matchDocument(doc, subFilter) {
					return false
				}
			}
		case "$or":
			subFilters, ok := value.([]Filter)
			if !ok {
				return false
			}
			// At least one subfilter must match
			for _, subFilter := range subFilters {
				if matchDocument(doc, subFilter) {
					return true
				}
			}
			return false
		case "$nor":
			subFilters, ok := value.([]Filter)
			if !ok {
				return false
			}
			// None of the subfilters should match
			for _, subFilter := range subFilters {
				if matchDocument(doc, subFilter) {
					return false
				}
			}
			return true
		default:
			if !applyOperator(doc, field, value) {
				return false
			}
		}
	}
	return true
}

// applyOperator applies a filter condition on a document field
func applyOperator(doc map[string]interface{}, field string, value interface{}) bool {
	keys := strings.Split(field, ".")
	fieldValue := getFindNestedValue(doc, keys)

	switch v := value.(type) {
	case Filter:
		for op, opVal := range v {
			switch op {
			case "$gt":
				return compare(fieldValue, opVal) > 0
			case "$lt":
				return compare(fieldValue, opVal) < 0
			case "$gte":
				return compare(fieldValue, opVal) >= 0
			case "$lte":
				return compare(fieldValue, opVal) <= 0
			case "$in":
				return isIn(fieldValue, opVal)
			case "$nin":
				return !isIn(fieldValue, opVal)
			case "$ne":
				return !reflect.DeepEqual(fieldValue, opVal)
			case "$exists":
				// For $exists, opVal should be a boolean indicating presence or absence
				exists := fieldValue != nil
				return reflect.DeepEqual(exists, opVal)
			case "$type":
				// For $type, opVal is expected to be a string type name (like "string", "int", etc.)
				return checkType(fieldValue, opVal)
			case "$regex":
				// For $regex, opVal should be a regular expression pattern string
				return applyRegex(fieldValue, opVal)
			case "$not":
				// For $not, opVal is a sub-query that must return false
				return !applyOperator(doc, field, opVal)
			default:
				return false
			}
		}
	default:
		// Equality check for simple field queries
		return reflect.DeepEqual(fieldValue, value)
	}
	return false
}

// compare compares two values and returns -1, 0, or 1
func compare(a, b interface{}) int {
	switch a := a.(type) {
	case float64:
		if b, ok := b.(float64); ok {
			if a < b {
				return -1
			} else if a > b {
				return 1
			}
			return 0
		}
	case time.Time:
		if b, ok := b.(time.Time); ok {
			if a.Before(b) {
				return -1
			} else if a.After(b) {
				return 1
			}
			return 0
		}
	}
	// Add more type comparisons as needed
	return -1
}

// checkType checks if the value is of a specific type (e.g., string, int, etc.)
func checkType(fieldValue interface{}, expectedType interface{}) bool {
	expectedTypeStr, ok := expectedType.(string)
	if !ok {
		return false
	}

	switch expectedTypeStr {
	case "string":
		_, ok := fieldValue.(string)
		return ok
	case "int":
		_, ok := fieldValue.(int)
		return ok
	case "float":
		_, ok := fieldValue.(float64)
		return ok
	case "bool":
		_, ok := fieldValue.(bool)
		return ok
	case "time":
		_, ok := fieldValue.(time.Time)
		return ok
	default:
		return false
	}
}

// applyRegex applies a regular expression match to a string field
func applyRegex(fieldValue interface{}, pattern interface{}) bool {
	strValue, ok := fieldValue.(string)
	if !ok {
		return false
	}

	patternStr, ok := pattern.(string)
	if !ok {
		return false
	}

	matched, err := regexp.MatchString(patternStr, strValue)
	if err != nil {
		return false
	}
	return matched
}

// isIn checks if a value is in a list
func isIn(fieldValue interface{}, values interface{}) bool {
	vals, ok := values.([]interface{})
	if !ok {
		return false
	}
	for _, v := range vals {
		if reflect.DeepEqual(fieldValue, v) {
			return true
		}
	}
	return false
}

// getNestedValue gets the value of a nested field from a document
func getFindNestedValue(doc map[string]interface{}, keys []string) interface{} {
	m := doc
	for _, key := range keys {
		if nested, ok := m[key].(map[string]interface{}); ok {
			m = nested
		} else {
			return m[key]
		}
	}
	return m
}

// Function to remove excluded fields from the document by following the full key path
func removeExcludedFields(doc map[string]interface{}, selectMap map[string]bool, currentPath string) {
	for key, value := range doc {
		// Construct the full key path for nested fields (e.g., "ratings.score")
		fullKeyPath := key
		if currentPath != "" {
			fullKeyPath = currentPath + "." + key
		}

		// If the current key or its full path is excluded, delete it
		if isExcluded(fullKeyPath, selectMap) {
			delete(doc, key)
			continue
		}

		// If the value is a nested map, recurse into it
		if nestedMap, ok := value.(map[string]interface{}); ok {
			// Recursively remove excluded fields from the nested map
			removeExcludedFields(nestedMap, selectMap, fullKeyPath)

			// If the nested map becomes empty after removing fields, delete the key
			if len(nestedMap) == 0 {
				delete(doc, key)
			}
		}
	}
}

// Helper function to check if a field or its full path is excluded
func isExcluded(key string, selectMap map[string]bool) bool {
	// Check if the key is marked as false in the selectMap
	if exclude, found := selectMap[key]; found {
		return !exclude // Return true if the key is marked as false
	}

	// Also check if any parent key (nested) is marked as false
	parts := strings.Split(key, ".")
	for i := 1; i < len(parts); i++ {
		nestedKey := strings.Join(parts[:i], ".")
		if exclude, found := selectMap[nestedKey]; found {
			return !exclude
		}
	}

	return false
}
