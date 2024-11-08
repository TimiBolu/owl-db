package utils

func RemoveDuplicates[T comparable](arr []T) []T {
	// Create a map to track unique values
	uniqueMap := make(map[T]bool)
	result := []T{}

	// Iterate over the array
	for _, v := range arr {
		// If the value is not in the map, add it to the result slice
		if !uniqueMap[v] {
			uniqueMap[v] = true
			result = append(result, v)
		}
	}

	return result
}
