package transport

import (
	"encoding/json"
	"log"
)

// MustMarshal serializa una estructura y aborta en errores de codificacion.
func MustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("marshal payload: %v", err)
	}
	return data
}

// MustUnmarshal deserializa payload JSON y aborta en errores de formato.
func MustUnmarshal(data []byte, v any) {
	if err := json.Unmarshal(data, v); err != nil {
		log.Fatalf("unmarshal payload: %v", err)
	}
}
