package model

import (
	"fmt"
	"math"
	"strings"
	"time"

	"proy-distri/internal/config"
)

// modelTimeZone unifica el huso horario operativo del dominio.
var modelTimeZone = time.FixedZone("UTC-5", -5*60*60)

// nowModelTime devuelve la hora de referencia usada en el modelo de ciudad.
func nowModelTime() time.Time {
	return time.Now().In(modelTimeZone)
}

// IntersectionState mantiene el estado operativo actual de una interseccion.
type IntersectionState struct {
	ID             string
	Row            string
	Col            int
	HasSemaphore   bool
	Upstream       string
	QueueLength    int
	AvgSpeed       float64
	Density        float64
	VehiclesCount  int
	LightState     string
	Status         string
	LastUpdate     time.Time
	LastTransition time.Time
}

// City representa el estado global de la malla de intersecciones.
type City struct {
	Name          string
	Rows          int
	Cols          int
	BaseGreenSec  int
	Intersections map[string]*IntersectionState
}

// NewCity crea el estado inicial de ciudad a partir de la configuracion.
func NewCity(cfg config.CityConfig) *City {
	city := &City{
		Name:          cfg.CityName,
		Rows:          cfg.MatrixRows,
		Cols:          cfg.MatrixCols,
		BaseGreenSec:  cfg.BaseGreenSeconds,
		Intersections: make(map[string]*IntersectionState, len(cfg.Intersections)),
	}

	for _, item := range cfg.Intersections {
		light := LightPhaseNone
		if item.HasSemaphore {
			light = PreferredPhaseForIntersection(item.Row, item.Col)
		}

		city.Intersections[item.ID] = &IntersectionState{
			ID:           item.ID,
			Row:          item.Row,
			Col:          item.Col,
			HasSemaphore: item.HasSemaphore,
			Upstream:     item.Upstream,
			LightState:   light,
			Status:       "NORMAL",
			LastUpdate:   nowModelTime(),
		}
	}

	return city
}

// Get obtiene una interseccion por ID.
func (c *City) Get(id string) (*IntersectionState, bool) {
	item, ok := c.Intersections[id]
	return item, ok
}

// UpdateFromCamera aplica datos provenientes de sensores de camara.
func (c *City) UpdateFromCamera(intersection string, volume int, speed float64) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	item.QueueLength = volume
	item.AvgSpeed = speed
	item.LastUpdate = nowModelTime()

	return c.snapshot(item), nil
}

// UpdateFromGPS aplica datos de congestion y velocidad desde GPS.
func (c *City) UpdateFromGPS(intersection string, density, speed float64, status string) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	item.Density = density
	item.AvgSpeed = speed
	item.LastUpdate = nowModelTime()
	item.Status = status

	return c.snapshot(item), nil
}

// UpdateFromInductive aplica conteos de sensores inductivos.
func (c *City) UpdateFromInductive(intersection string, counted int) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	item.VehiclesCount = counted
	item.LastUpdate = nowModelTime()

	return c.snapshot(item), nil
}

// ApplyInfluence ajusta el estado local segun el semaforo aguas arriba.
func (c *City) ApplyInfluence(intersection string) {
	item, ok := c.Get(intersection)
	if !ok || item.Upstream == "" {
		return
	}

	upstream, ok := c.Get(item.Upstream)
	if !ok {
		return
	}

	axis := flowAxisBetween(upstream, item)
	if axis == "" {
		return
	}

	if phaseAllowsAxis(upstream.LightState, axis) && item.HasSemaphore && !phaseAllowsAxis(item.LightState, axis) {
		item.QueueLength += 2
		item.AvgSpeed = math.Max(5, item.AvgSpeed-3)
		item.Density += 4
	}

	if phaseAllowsAxis(item.LightState, axis) && item.QueueLength > 0 {
		item.QueueLength = int(math.Max(0, float64(item.QueueLength-1)))
	}
}

// SetLight cambia la fase del semaforo de una interseccion.
func (c *City) SetLight(intersection, state string) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	if !item.HasSemaphore {
		return nil, fmt.Errorf("intersection %s has no semaphore", intersection)
	}

	phase := strings.ToUpper(state)
	if phase != LightPhaseVertical && phase != LightPhaseHorizontal {
		return nil, fmt.Errorf("invalid light phase %s", state)
	}

	item.LightState = phase
	item.LastTransition = nowModelTime()

	return c.snapshot(item), nil
}

// SetStatus actualiza el estado semantico de una interseccion.
func (c *City) SetStatus(intersection, status string) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	previousStatus := item.Status
	if previousStatus == "PRIORITY" && status == "CONGESTION" {
		item.Status = "CONGESTION_BUT_PRIORITY"
	} else if previousStatus == "CONGESTION_BUT_PRIORITY" && status == "NORMAL" {
		item.Status = "PRIORITY"
	} else {
		item.Status = status
	}

	item.LastUpdate = nowModelTime()
	return c.snapshot(item), nil
}

// SnapshotAll devuelve una vista del estado actual para todas las intersecciones.
func (c *City) SnapshotAll() []IntersectionSnapshot {
	out := make([]IntersectionSnapshot, 0, len(c.Intersections))
	for _, item := range c.Intersections {
		out = append(out, *c.snapshot(item))
	}
	return out
}

// snapshot construye una vista serializable desde un estado interno.
func (c *City) snapshot(item *IntersectionState) *IntersectionSnapshot {
	return &IntersectionSnapshot{
		Intersection:    item.ID,
		QueueLength:     item.QueueLength,
		AvgSpeed:        item.AvgSpeed,
		Density:         item.Density,
		VehiclesCounted: item.VehiclesCount,
		LightState:      item.LightState,
		HasSemaphore:    item.HasSemaphore,
		Status:          item.Status,
		UpdatedAt:       item.LastUpdate,
	}
}

// PreferredPhaseForIntersection define la fase preferida base por interseccion.
func PreferredPhaseForIntersection(row string, col int) string {
	return LightPhaseHorizontal
}

// PreferredPhaseForIntersectionID calcula la fase preferida desde un ID textual.
func PreferredPhaseForIntersectionID(id string) string {
	row, col, ok := parseIntersectionID(id)
	if !ok {
		return LightPhaseVertical
	}
	return PreferredPhaseForIntersection(row, col)
}

// OppositePhase devuelve la fase opuesta para alternancia de semaforo.
func OppositePhase(phase string) string {
	switch strings.ToUpper(phase) {
	case LightPhaseVertical:
		return LightPhaseHorizontal
	case LightPhaseHorizontal:
		return LightPhaseVertical
	default:
		return LightPhaseNone
	}
}

// phaseAllowsAxis indica si una fase habilita un eje de flujo.
func phaseAllowsAxis(phase, axis string) bool {
	phase = strings.ToUpper(phase)
	axis = strings.ToUpper(axis)
	return (phase == LightPhaseVertical && axis == FlowAxisVertical) ||
		(phase == LightPhaseHorizontal && axis == FlowAxisHorizontal)
}

// flowAxisBetween determina el eje compartido entre dos intersecciones.
func flowAxisBetween(upstream, current *IntersectionState) string {
	switch {
	case upstream.Row == current.Row:
		return FlowAxisHorizontal
	case upstream.Col == current.Col:
		return FlowAxisVertical
	default:
		return ""
	}
}

// parseIntersectionID separa fila y columna desde IDs del tipo INT_A3.
func parseIntersectionID(id string) (string, int, bool) {
	label := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(id)), "INT_")
	if len(label) < 2 {
		return "", 0, false
	}

	row := string(label[0])
	colValue := 0
	for _, ch := range label[1:] {
		if ch < '0' || ch > '9' {
			return "", 0, false
		}
		colValue = (colValue * 10) + int(ch-'0')
	}

	return row, colValue, true
}
