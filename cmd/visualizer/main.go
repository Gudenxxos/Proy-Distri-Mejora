package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
	"proy-distri/internal/web"
)

func main() {
	cfgPath := getenv("CITY_CONFIG", "configs/city.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	app := newVisualizer(cfg)
	go app.consumeBroker()

	http.HandleFunc("/", app.handleIndex)
	http.HandleFunc("/api/state", app.handleState)
	http.HandleFunc("/events", app.handleEvents)

	log.Printf("[visualizer] http en %s", cfg.Endpoints.VisualizerHTTP)
	log.Fatal(http.ListenAndServe(cfg.Endpoints.VisualizerHTTP, nil))
}

type visualizer struct {
	cfg   config.CityConfig
	mu    sync.RWMutex
	state map[string]model.IntersectionSnapshot
	subs  map[chan []byte]struct{}
}

func newVisualizer(cfg config.CityConfig) *visualizer {
	state := make(map[string]model.IntersectionSnapshot, len(cfg.Intersections))
	for _, item := range cfg.Intersections {
		light := model.LightPhaseNone
		if item.HasSemaphore {
			light = model.PreferredPhaseForIntersection(item.Row, item.Col)
		}
		state[item.ID] = model.IntersectionSnapshot{
			Intersection: item.ID,
			HasSemaphore: item.HasSemaphore,
			LightState:   light,
			Status:       "NORMAL",
		}
	}

	return &visualizer{
		cfg:   cfg,
		state: state,
		subs:  make(map[chan []byte]struct{}),
	}
}

func (v *visualizer) consumeBroker() {
	sub := zmq4.NewSub(context.Background())
	defer sub.Close()

	if err := sub.Dial(v.cfg.Endpoints.BrokerFanout); err != nil {
		log.Fatalf("visualizer dial fanout: %v", err)
	}
	for _, topic := range []string{model.TopicSnapshot, model.TopicCommand, model.TopicLightState} {
		if err := sub.SetOption(zmq4.OptionSubscribe, topic); err != nil {
			log.Fatalf("visualizer subscribe %s: %v", topic, err)
		}
	}

	for {
		msg, err := sub.Recv()
		if err != nil {
			log.Printf("visualizer recv: %v", err)
			return
		}
		if len(msg.Frames) < 2 {
			continue
		}
		topic := string(msg.Frames[0])
		payload := msg.Frames[1]

		v.mu.Lock()
		switch topic {
		case model.TopicSnapshot:
			var env model.PersistEnvelope
			if json.Unmarshal(payload, &env) == nil && env.Snapshot != nil {
				v.state[env.Snapshot.Intersection] = *env.Snapshot
			}
		case model.TopicLightState:
			var event model.LightStateEvent
			if json.Unmarshal(payload, &event) == nil {
				item := v.state[event.Intersection]
				item.Intersection = event.Intersection
				item.LightState = event.LightState
				v.state[event.Intersection] = item
			}
		}
		v.broadcast(payload, topic)
		v.mu.Unlock()
	}
}

func (v *visualizer) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(web.IndexHTML))
}

func (v *visualizer) handleState(w http.ResponseWriter, _ *http.Request) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	list := make([]model.IntersectionSnapshot, 0, len(v.state))
	for _, item := range v.state {
		list = append(list, item)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

func (v *visualizer) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 16)
	v.mu.Lock()
	v.subs[ch] = struct{}{}
	v.mu.Unlock()

	defer func() {
		v.mu.Lock()
		delete(v.subs, ch)
		v.mu.Unlock()
		close(ch)
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (v *visualizer) broadcast(payload []byte, topic string) {
	envelope := map[string]any{"topic": topic}
	if topic == model.TopicSnapshot {
		var env model.PersistEnvelope
		if json.Unmarshal(payload, &env) == nil {
			envelope["snapshot"] = env.Snapshot
		}
	}
	if topic == model.TopicCommand {
		var cmd model.LightCommand
		if json.Unmarshal(payload, &cmd) == nil {
			envelope["light_command"] = cmd
		}
	}
	if topic == model.TopicLightState {
		var event model.LightStateEvent
		if json.Unmarshal(payload, &event) == nil {
			envelope["light_state"] = event
		}
	}
	data, _ := json.Marshal(envelope)
	for ch := range v.subs {
		select {
		case ch <- data:
		default:
		}
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
