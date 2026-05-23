package analytics

import (
	"testing"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
)

func TestEvaluateCongestion(t *testing.T) {
	e := NewEvaluator(config.CityConfig{
		BaseGreenSeconds:           15,
		CongestionExtensionSeconds: 10,
	})

	status, cmd := e.Evaluate(model.IntersectionSnapshot{
		Intersection: "INT_B3",
		QueueLength:  10,
		AvgSpeed:     15,
		Density:      40,
		HasSemaphore: true,
	})

	if status != StatusCongestion {
		t.Fatalf("expected congestion, got %s", status)
	}
	if cmd == nil || cmd.DurationSec != 25 || cmd.TargetState != model.LightPhaseHorizontal {
		t.Fatalf("unexpected command: %+v", cmd)
	}
}

func TestBuildPriorityWave(t *testing.T) {
	city := &model.City{Intersections: map[string]*model.IntersectionState{
		"INT_B2": {ID: "INT_B2", HasSemaphore: true},
		"INT_B3": {ID: "INT_B3", HasSemaphore: true},
		"INT_C3": {ID: "INT_C3", HasSemaphore: true},
		"INT_C2": {ID: "INT_C2", HasSemaphore: false},
	}}
	e := NewEvaluator(config.CityConfig{PriorityWaveSeconds: 30})

	commands := e.BuildPriorityWave("B", city)
	if len(commands) != 2 {
		t.Fatalf("expected 2 commands for route B, got %d", len(commands))
	}
	if commands[0].TargetState != model.LightPhaseHorizontal {
		t.Fatalf("expected horizontal phase for route B, got %s", commands[0].TargetState)
	}
}
