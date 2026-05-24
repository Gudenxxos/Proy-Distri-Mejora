package model

import (
	"fmt"
	"math"
	"strings"
	"time"

	"proy-distri/internal/config"
)

var modelTimeZone = time.FixedZone("UTC-5", -5*60*60)

func nowModelTime() time.Time {
	return time.Now().In(modelTimeZone)
}

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

type City struct {
	Name          string
	Rows          int
	Cols          int
	BaseGreenSec  int
	Intersections map[string]*IntersectionState
}

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

func (c *City) Get(id string) (*IntersectionState, bool) {
	item, ok := c.Intersections[id]
	return item, ok
}

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

func (c *City) UpdateFromGPS(intersection string, density, speed float64) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	item.Density = density
	item.AvgSpeed = speed
	item.LastUpdate = nowModelTime()

	return c.snapshot(item), nil
}

func (c *City) UpdateFromInductive(intersection string, counted int) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	item.VehiclesCount = counted
	item.QueueLength = int(math.Max(0, float64(item.QueueLength-counted)))
	item.LastUpdate = nowModelTime()

	return c.snapshot(item), nil
}

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

func (c *City) SetStatus(intersection, status string) (*IntersectionSnapshot, error) {
	item, ok := c.Get(intersection)
	if !ok {
		return nil, fmt.Errorf("intersection %s not found", intersection)
	}

	item.Status = strings.ToUpper(status)
	item.LastUpdate = nowModelTime()
	return c.snapshot(item), nil
}

func (c *City) SnapshotAll() []IntersectionSnapshot {
	out := make([]IntersectionSnapshot, 0, len(c.Intersections))
	for _, item := range c.Intersections {
		out = append(out, *c.snapshot(item))
	}
	return out
}

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

func PreferredPhaseForIntersection(row string, col int) string {
	return LightPhaseHorizontal
}

func PreferredPhaseForIntersectionID(id string) string {
	row, col, ok := parseIntersectionID(id)
	if !ok {
		return LightPhaseVertical
	}
	return PreferredPhaseForIntersection(row, col)
}

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

func phaseAllowsAxis(phase, axis string) bool {
	phase = strings.ToUpper(phase)
	axis = strings.ToUpper(axis)
	return (phase == LightPhaseVertical && axis == FlowAxisVertical) ||
		(phase == LightPhaseHorizontal && axis == FlowAxisHorizontal)
}

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
