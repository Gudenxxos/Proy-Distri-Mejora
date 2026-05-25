package analytics

import (
	"testing"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
)

// TestEvaluateCongestion valida que una condicion de congestion emita comando.
func TestEvaluateCongestion(t *testing.T) {
	e := NewEvaluator(config.CityConfig{
		BaseGreenSeconds:           15,
		CongestionExtensionSeconds: 10,
		SensorProfiles: []config.SensorProfile{{
			SensorID:     "CAM-B3",
			SensorType:   "camara",
			Intersection: "INT_B3",
		}},
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

// TestEvaluateIgnoresInductiveOnlyIntersection validates that inductive-only nodes do not trigger congestion.
func TestEvaluateIgnoresInductiveOnlyIntersection(t *testing.T) {
	e := NewEvaluator(config.CityConfig{
		BaseGreenSeconds:           15,
		CongestionExtensionSeconds: 10,
		SensorProfiles: []config.SensorProfile{{
			SensorID:     "ESP-D3",
			SensorType:   "espira_inductiva",
			Intersection: "INT_D3",
		}},
	})

	status, cmd := e.Evaluate(model.IntersectionSnapshot{
		Intersection: "INT_D3",
		QueueLength:  10,
		AvgSpeed:     0,
		Density:      40,
		HasSemaphore: true,
	})

	if status != StatusNormal {
		t.Fatalf("expected normal for inductive-only intersection, got %s", status)
	}
	if cmd != nil {
		t.Fatalf("expected no command for inductive-only intersection, got %+v", cmd)
	}
}

// TestBuildPriorityWave valida la seleccion de semaforos por ruta priorizada.
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

// TestBuildForceGreen valida la construccion de un comando manual force_green.
func TestBuildForceGreen(t *testing.T) {
	e := NewEvaluator(config.CityConfig{
		Intersections: []config.IntersectionConfig{
			{ID: "INT_B3", Row: "B", Col: 3, HasSemaphore: true},
			{ID: "INT_A1", Row: "A", Col: 1, HasSemaphore: false},
		},
	})

	cmd, err := e.BuildForceGreen("INT_B3", 12, model.LightPhaseVertical)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Intersection != "INT_B3" {
		t.Fatalf("unexpected intersection: %s", cmd.Intersection)
	}
	if cmd.DurationSec != 12 {
		t.Fatalf("unexpected duration: %d", cmd.DurationSec)
	}
	if cmd.TargetState != model.LightPhaseHorizontal {
		t.Fatalf("unexpected phase: %s", cmd.TargetState)
	}
	if cmd.Reason != ReasonForceGreen {
		t.Fatalf("unexpected reason: %s", cmd.Reason)
	}
}
