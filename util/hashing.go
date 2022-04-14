package util

import (
	"encoding/binary"
	"hash/fnv"
)

func HashId(vertexId uint64) uint64 {
	inputBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(inputBytes, vertexId)

	algorithm := fnv.New64a()
	algorithm.Write(inputBytes)
	return algorithm.Sum64()
}
