package storage

import (
	"errors"
	"testing"
)

type fakeClient struct {
	data []byte
	err  error
}

func (f fakeClient) QueryCurrent(intersection string) ([]byte, error) {
	return f.data, f.err
}

func (f fakeClient) QueryHistory(payload []byte) ([]byte, error) {
	return f.data, f.err
}

func TestRouterFallsBackToReplica(t *testing.T) {
	router := Router{
		Primary: fakeClient{err: errors.New("down")},
		Replica: fakeClient{data: []byte(`{"success":true}`)},
	}

	data, err := router.QueryCurrent("INT_B3")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"success":true}` {
		t.Fatalf("unexpected data: %s", data)
	}
}
