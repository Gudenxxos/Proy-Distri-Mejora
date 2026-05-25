package storage

import "errors"

type QueryClient interface {
	QueryCurrent(intersection string) ([]byte, error)
	QueryHistory(payload []byte) ([]byte, error)
}

type Router struct {
	Primary QueryClient
	Replica QueryClient
}

func (r Router) QueryCurrent(intersection string) ([]byte, error) {
	if r.Primary != nil {
		if data, err := r.Primary.QueryCurrent(intersection); err == nil {
			return data, nil
		}
	}

	if r.Replica != nil {
		return r.Replica.QueryCurrent(intersection)
	}

	return nil, errors.New("no database client available")
}

func (r Router) QueryHistory(payload []byte) ([]byte, error) {
	if r.Primary != nil {
		if data, err := r.Primary.QueryHistory(payload); err == nil {
			return data, nil
		}
	}

	if r.Replica != nil {
		return r.Replica.QueryHistory(payload)
	}

	return nil, errors.New("no database client available")
}
