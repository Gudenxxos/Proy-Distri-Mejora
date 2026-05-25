package model

import (
	"testing"

	"proy-distri/internal/config"
)

func TestCityUpdateAndInfluence(t *testing.T) {
	city := NewCity(config.CityConfig{
		CityName:         "test",
		MatrixRows:       1,
		MatrixCols:       2,
		BaseGreenSeconds: 15,
		Intersections: []config.IntersectionConfig{
			{ID: "INT_A1", Row: "A", Col: 1, HasSemaphore: true},
			{ID: "INT_A2", Row: "A", Col: 2, HasSemaphore: true, Upstream: "INT_A1"},
		},
	})

	if _, err := city.SetLight("INT_A1", LightPhaseHorizontal); err != nil {
		t.Fatal(err)
	}
	if _, err := city.SetLight("INT_A2", LightPhaseVertical); err != nil {
		t.Fatal(err)
	}
	if _, err := city.UpdateFromCamera("INT_A2", 5, 30); err != nil {
		t.Fatal(err)
	}

	city.ApplyInfluence("INT_A2")
	item, _ := city.Get("INT_A2")
	if item.QueueLength <= 5 {
		t.Fatalf("expected queue growth from upstream green, got %d", item.QueueLength)
	}
}
