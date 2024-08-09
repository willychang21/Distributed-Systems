package mr

import (
	"encoding/json"
	"hash/fnv"
	"os"
)

// ihash is a hash function for string keys.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// writeToJsonFile writes a slice of KeyValue pairs to a JSON file.
func writeToJsonFile(file *os.File, kva []KeyValue) error {
	enc := json.NewEncoder(file)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			return err
		}
	}
	return nil
}

// readFromJsonFile reads a slice of KeyValue pairs from a JSON file.
func readFromJsonFile(file *os.File) []KeyValue {
	dec := json.NewDecoder(file)
	var kva []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}
