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
	return uint64(algorithm.Sum64())
}

func GetFlooredModulo(a uint64, b uint64) uint64 {
	return ((a % b) + b) % b
}
