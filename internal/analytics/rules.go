package analytics

import (
	"fmt"
	"strings"
	"time"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
)

var analyticsTimeZone = time.FixedZone("UTC-5", -5*60*60)

func nowAnalyticsTime() time.Time {
	return time.Now().In(analyticsTimeZone)
}

const (
	StatusNormal      = "NORMAL"
	StatusCongestion  = "CONGESTION"
	StatusPriority    = "PRIORITY"
	ReasonNormal      = "trafico_normal"
	ReasonCongestion  = "deteccion_congestion"
	ReasonPriority    = "ola_verde"
	RequestAnalytics  = "analytics"
	RequestMonitoring = "monitoring"
)

type Evaluator struct {
	cfg config.CityConfig
}

func NewEvaluator(cfg config.CityConfig) Evaluator {
	return Evaluator{cfg: cfg}
}

func (e Evaluator) Evaluate(snapshot model.IntersectionSnapshot) (string, *model.LightCommand) {
	switch {
	case snapshot.QueueLength >= 8 || snapshot.AvgSpeed < 20 || snapshot.Density >= 35:
		duration := e.cfg.BaseGreenSeconds + e.cfg.CongestionExtensionSeconds
		targetPhase := model.PreferredPhaseForIntersectionID(snapshot.Intersection)
		return StatusCongestion, &model.LightCommand{
			CommandID:    fmt.Sprintf("cmd-%d", time.Now().UnixNano()),
			Intersection: snapshot.Intersection,
			TargetState:  targetPhase,
			DurationSec:  duration,
			Reason:       ReasonCongestion,
			RequestedBy:  RequestAnalytics,
			RequestedAt:  nowAnalyticsTime(),
		}
	case snapshot.QueueLength < 5 && snapshot.AvgSpeed > 35 && snapshot.Density < 20 && snapshot.HasSemaphore:
		targetPhase := model.PreferredPhaseForIntersectionID(snapshot.Intersection)
		return StatusNormal, &model.LightCommand{
			CommandID:    fmt.Sprintf("cmd-%d", time.Now().UnixNano()),
			Intersection: snapshot.Intersection,
			TargetState:  targetPhase,
			DurationSec:  e.cfg.BaseGreenSeconds,
			Reason:       ReasonNormal,
			RequestedBy:  RequestAnalytics,
			RequestedAt:  nowAnalyticsTime(),
		}
	default:
		return StatusNormal, nil
	}
}

func (e Evaluator) BuildPriorityWave(route string, city *model.City) []model.LightCommand {
	route = strings.ToUpper(strings.TrimSpace(route))
	commands := make([]model.LightCommand, 0)

	for _, item := range city.SnapshotAll() {
		if !item.HasSemaphore {
			continue
		}

		label := strings.TrimPrefix(item.Intersection, "INT_")
		runes := []rune(label)
		if len(runes) < 2 {
			continue
		}
		row := string(runes[0])
		col := string(runes[1])

		if route == row || route == col {
			targetPhase := model.LightPhaseHorizontal
			if route == col {
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
