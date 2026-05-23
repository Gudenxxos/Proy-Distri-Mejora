package transport

import (
	"encoding/json"
	"log"
)

func MustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("marshal payload: %v", err)
	}
	return data
}

func MustUnmarshal(data []byte, v any) {
	if err := json.Unmarshal(data, v); err != nil {
		log.Fatalf("unmarshal payload: %v", err)
	}
}
