package database

/*
func toBatchWriteItem(tableName string, graph Graph) [][]dynamodb.BatchWriteItemInput {
	numBatches := int(math.Ceil(float64(len(graph)) / float64(MAXIMUM_ITEMS_PER_BATCH)))
	batches := make([][]dynamodb.BatchWriteItemInput, numBatches)

	// 2D Batch Array, batch[BATCH #][ITEM #]
	for batchNum := 0; batchNum < numBatches; batchNum++ {
		if batchNum == numBatches - 1 {
			batches[batchNum] = make([]dynamodb.BatchWriteItemInput, len(graph) % MAXIMUM_ITEMS_PER_BATCH)
			continue
		}
		batches[batchNum] = make([]dynamodb.BatchWriteItemInput, MAXIMUM_ITEMS_PER_BATCH)
	}

	itemIdx := 0
	for srcVertex, neighbors := range graph {
		batchNum := itemIdx / MAXIMUM_ITEMS_PER_BATCH
		batchPos := itemIdx % MAXIMUM_ITEMS_PER_BATCH
		batches[batchNum][batchPos] = types.PutRequest{Item: map[string]}


	}

	return *batch
}


*/
