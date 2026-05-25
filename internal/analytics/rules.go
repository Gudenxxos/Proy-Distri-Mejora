package analytics

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
)

// analyticsTimeZone fija la referencia horaria para decisiones del evaluador.
var analyticsTimeZone = time.FixedZone("UTC-5", -5*60*60)

// nowAnalyticsTime devuelve la hora operacional del motor de reglas.
func nowAnalyticsTime() time.Time {
	return time.Now().In(analyticsTimeZone)
}

// Constantes de clasificacion y motivos de decision.
const (
	StatusNormal      = "NORMAL"
	StatusCongestion  = "CONGESTION"
	StatusPriority    = "PRIORITY"
	ReasonNormal      = "trafico_normal"
	ReasonCongestion  = "deteccion_congestion"
	ReasonPriority    = "ola_verde"
	ReasonForceGreen  = "force_green"
	RequestAnalytics  = "analytics"
	RequestMonitoring = "monitoring"
)

// Evaluator aplica reglas de negocio sobre snapshots de intersecciones.
type Evaluator struct {
	cfg config.CityConfig
}

// NewEvaluator crea un evaluador con la configuracion operativa actual.
func NewEvaluator(cfg config.CityConfig) Evaluator {
	return Evaluator{cfg: cfg}
}

// Evaluate examines the intersection snapshot and determines if congestion is detected.
// It applies row/column level congestion logic and returns the status and command.
// Returns:
//   - Status: NORMAL, CONGESTION, or PRIORITY
//   - Command: Light command to apply at row/column level, or nil if no action needed
func (e Evaluator) Evaluate(snapshot model.IntersectionSnapshot) (string, *model.LightCommand) {
	switch {
	case snapshot.QueueLength >= 8 || snapshot.AvgSpeed < 20 || snapshot.Density >= 35:
		// Congestion detected in this intersection - apply row/column wide command
		duration := e.cfg.BaseGreenSeconds + e.cfg.CongestionExtensionSeconds
		return StatusCongestion, &model.LightCommand{
			CommandID:    fmt.Sprintf("cmd-%d", time.Now().UnixNano()),
			Intersection: snapshot.Intersection,
			TargetState:  e.determinePhaseForIntersection(snapshot.Intersection, snapshot.LightState),
			DurationSec:  duration,
			Reason:       ReasonCongestion,
			RequestedBy:  RequestAnalytics,
			RequestedAt:  nowAnalyticsTime(),
		}
	/* case snapshot.QueueLength < 5 && snapshot.AvgSpeed > 35 && snapshot.Density < 20 && snapshot.HasSemaphore:
		duration := e.cfg.BaseGreenSeconds
		return StatusNormal, &model.LightCommand{
			CommandID:    fmt.Sprintf("cmd-%d", time.Now().UnixNano()),
			Intersection: snapshot.Intersection,
			TargetState:  e.determinePhaseForIntersection(snapshot.Intersection, snapshot.LightState),
			DurationSec:  duration,
			Reason:       ReasonNormal,
			RequestedBy:  RequestAnalytics,
			RequestedAt:  nowAnalyticsTime(),
		} */
	default:
		return StatusNormal, nil
	}
}

// determinePhaseForIntersection define la siguiente fase para una interseccion.
func (e Evaluator) determinePhaseForIntersection(intersectionID string, currentPhase string) string {
// Si el estado actual es HORIZONTAL, return VERTICAL
	// Si el estado actual es VERTICAL, return HORIZONTAL
	switch currentPhase {
	case model.LightPhaseHorizontal:
		return model.LightPhaseVertical
	case model.LightPhaseVertical:
		return model.LightPhaseHorizontal
	default:
		return model.LightPhaseHorizontal
	}
}
	
	
	


// parseIntersectionID descompone IDs de interseccion en fila y columna.
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

// BuildPriorityWave genera comandos de prioridad para una fila o columna.
func (e Evaluator) BuildPriorityWave(route string, city *model.City) []model.LightCommand {
	route = strings.ToUpper(strings.TrimSpace(route))
	commands := make([]model.LightCommand, 0)

	// Determine if route is a row or column
	isRow := len(route) == 1 && route >= "A" && route <= "Z"
	isCol := false
	colNum := 0
	if !isRow {
		// Try to parse as column number
		if n, err := strconv.Atoi(route); err == nil {
			isCol = true
			colNum = n
		}
	}

	for _, item := range city.SnapshotAll() {
		if !item.HasSemaphore {
			continue
		}

		label := strings.TrimPrefix(item.Intersection, "INT_")
		runes := []rune(label)
		if len(runes) < 2 {
			continue
		}
		rowChar := string(runes[0])
		colStr := string(runes[1:])
		colVal := 0
		fmt.Sscanf(colStr, "%d", &colVal)

		// Check if this intersection matches the route
		matches := false
		if isRow && rowChar == route {
			matches = true
		} else if isCol && colVal == colNum {
			matches = true
		}

		if matches {
			// Determine phase based on whether it's row or column
			var targetPhase string
			if isRow {
				targetPhase = model.LightPhaseHorizontal
			} else if isCol {
				targetPhase = model.LightPhaseVertical
			} else {
				targetPhase = model.LightPhaseVertical
			}

			commands = append(commands, model.LightCommand{
				CommandID:     fmt.Sprintf("cmd-%d", time.Now().UnixNano()),
				Intersection:  item.Intersection,
				TargetState:   targetPhase,
				DurationSec:   e.cfg.PriorityWaveSeconds,
				Reason:        ReasonPriority,
				RequestedBy:   RequestMonitoring,
				RequestedAt:   time.Now().UTC(),
				PriorityRoute: route,
			})
		}
	}

	return commands
}

// BuildForceGreen construye un comando manual de prioridad temporal.
func (e Evaluator) BuildForceGreen(intersection string, durationSec int, currentPhase string) (model.LightCommand, error) {
	if durationSec <= 0 {
		return model.LightCommand{}, fmt.Errorf("duracion invalida para force_green: %d", durationSec)
	}
	if !e.hasSemaphoreIntersection(intersection) {
		return model.LightCommand{}, fmt.Errorf("intersection %s has no semaphore", intersection)
	}

	intersection = strings.ToUpper(strings.TrimSpace(intersection))

	return model.LightCommand{
		CommandID:    fmt.Sprintf("cmd-%d", time.Now().UnixNano()),
		Intersection: intersection,
		TargetState:  e.determinePhaseForIntersection(intersection, currentPhase),
		DurationSec:  durationSec,
		Reason:       ReasonForceGreen,
		RequestedBy:  RequestMonitoring,
		RequestedAt:  nowAnalyticsTime(),
	}, nil
}

// hasSemaphoreIntersection valida si la interseccion soporta comandos de semaforo.
func (e Evaluator) hasSemaphoreIntersection(intersectionID string) bool {
	intersectionID = strings.ToUpper(strings.TrimSpace(intersectionID))
	for _, item := range e.cfg.Intersections {
		if strings.ToUpper(strings.TrimSpace(item.ID)) == intersectionID {
			return item.HasSemaphore
		}
	}
	return false
}
