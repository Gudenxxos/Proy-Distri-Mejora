package storage

import "errors"

// QueryClient define la interfaz minima para consultas de monitoreo.
type QueryClient interface {
	QueryCurrent(intersection string) ([]byte, error)
	QueryHistory(payload []byte) ([]byte, error)
}

// Router enruta consultas al primario y aplica failover hacia replica.
type Router struct {
	Primary QueryClient
	Replica QueryClient
}

// QueryCurrent consulta estado puntual priorizando el primario.
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

// QueryHistory consulta historico con la misma estrategia de failover.
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
