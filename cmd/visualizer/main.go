package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
	"proy-distri/internal/web"
)

// main inicia el visualizador HTTP y los consumidores de eventos.
func main() {
	cfgPath := getenv("CITY_CONFIG", "configs/city.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	app := newVisualizer(cfg)
	go app.consumeBroker()
	go app.consumeLightCommands()

	http.HandleFunc("/", app.handleIndex)
	http.HandleFunc("/api/state", app.handleState)
	http.HandleFunc("/events", app.handleEvents)

	log.Printf("[visualizer] http en %s", cfg.Endpoints.VisualizerHTTP)
	log.Fatal(http.ListenAndServe(cfg.Endpoints.VisualizerHTTP, nil))
}

// visualizer mantiene estado de intersecciones y suscriptores SSE.
type visualizer struct {
	cfg   config.CityConfig
	mu    sync.RWMutex
	state map[string]model.IntersectionSnapshot
	subs  map[chan []byte]struct{}
}

// newVisualizer construye el estado inicial para la UI.
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

// hasSensor verifica si una interseccion tiene al menos un sensor asociado.
func (v *visualizer) hasSensor(intersection string) bool {
	intersection = strings.ToUpper(strings.TrimSpace(intersection))
	for _, sp := range v.cfg.SensorProfiles {
		if strings.ToUpper(strings.TrimSpace(sp.Intersection)) == intersection {
			return true
		}
	}
	return false
}

// hasSpeedSensor verifica si la interseccion puede reportar velocidad real.
func (v *visualizer) hasSpeedSensor(intersection string) bool {
	intersection = strings.ToUpper(strings.TrimSpace(intersection))
	for _, sp := range v.cfg.SensorProfiles {
		if strings.ToUpper(strings.TrimSpace(sp.Intersection)) != intersection {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(sp.SensorType)) {
		case "camara", "gps":
			return true
		}
	}
	return false
}

// consumeBroker procesa eventos de sensores y actualiza snapshots.
func (v *visualizer) consumeBroker() {
	sub := zmq4.NewSub(context.Background())
	defer sub.Close()

	if err := sub.Dial(v.cfg.Endpoints.BrokerFanout); err != nil {
		log.Fatalf("visualizer dial fanout: %v", err)
	}
	// Solo suscribirse a eventos de sensores
	for _, topic := range []string{model.TopicCamera, model.TopicGPS, model.TopicInductive} {
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
		var updated *model.IntersectionSnapshot
		// Procesar eventos de sensores para actualizar datos de intersecciones
		switch topic {
		case model.TopicCamera:
			var event model.CameraEvent
			if json.Unmarshal(payload, &event) == nil {
				item := v.state[event.Interseccion]
				item.Intersection = event.Interseccion
				item.QueueLength = event.Volumen
				item.AvgSpeed = event.VelocidadPromedio
				v.state[event.Interseccion] = item
				updated = &item
			}
		case model.TopicGPS:
			var event model.GPSEvent
			if json.Unmarshal(payload, &event) == nil {
				item := v.state[event.Interseccion]
				item.Intersection = event.Interseccion
				item.Density = event.Densidad
				item.AvgSpeed = event.VelocidadPromedio
				v.state[event.Interseccion] = item
				updated = &item
			}
		case model.TopicInductive:
			var event model.InductiveEvent
			if json.Unmarshal(payload, &event) == nil {
				item := v.state[event.Interseccion]
				item.Intersection = event.Interseccion
				item.VehiclesCounted = event.VehiculosContados
				v.state[event.Interseccion] = item
				updated = &item
			}
		}
		if updated != nil {
			item := v.state[updated.Intersection]
			item.Status = v.statusCalculatedFromData(item)
			v.state[updated.Intersection] = item
			updated = &item
			v.broadcastSnapshot(*updated, topic)
		}
		v.mu.Unlock()
	}
}

// statusCalculatedFromData deriva un estado visual desde metricas de trafico.
func (v *visualizer) statusCalculatedFromData(item model.IntersectionSnapshot) string {
	hasSpeed := v.hasSpeedSensor(item.Intersection)
	if item.QueueLength >= 8 || item.Density >= 35 {
		return "CONGESTION"
	}
	if hasSpeed && item.AvgSpeed > 0 && item.AvgSpeed < 20 {
		return "CONGESTION"
	}
	return "NORMAL"
}

// consumeLightCommands recibe LightCommand desde analytics para actualizar semáforos
func (v *visualizer) consumeLightCommands() {
	pull := zmq4.NewPull(context.Background())
	defer pull.Close()

	if err := pull.Listen(v.cfg.Endpoints.VisualizerLightPush); err != nil {
		log.Fatalf("visualizer listen light push: %v", err)
	}

	for {
		msg, err := pull.Recv()
		if err != nil {
			log.Printf("visualizer recv light command: %v", err)
			return
		}
		if len(msg.Frames) == 0 {
			continue
		}

		var cmd model.LightCommand
		if err := json.Unmarshal(msg.Frames[0], &cmd); err != nil {
			log.Printf("visualizer unmarshal light command: %v", err)
			continue
		}

		v.mu.Lock()
		item := v.state[cmd.Intersection]
		item.Intersection = cmd.Intersection
		item.LightState = cmd.TargetState
		item.Status = v.statusFromReason(item.Status, cmd.Reason)
		v.state[cmd.Intersection] = item

		v.broadcastSnapshot(item, "light_command")
		v.mu.Unlock()

		log.Printf("[visualizer] actualizado %s => %s", cmd.Intersection, cmd.TargetState)
	}
}

// broadcastSnapshot publica una actualizacion a clientes SSE activos.
func (v *visualizer) broadcastSnapshot(snapshot model.IntersectionSnapshot, topic string) {
	envelope := map[string]any{
		"topic":    topic,
		"snapshot": snapshot,
	}
	data, _ := json.Marshal(envelope)
	for ch := range v.subs {
		select {
		case ch <- data:
		default:
		}
	}
}

// statusFromReason traduce razones de comando a estado de UI.
func (v *visualizer) statusFromReason(previous, reason string) string {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "deteccion_congestion":
		return "SOLVED"
	case "force_green":
		return "PRIORITY"
	case "ola_verde":
		return "PRIORITY"
	case "cycle_end":
		return "NORMAL"
	default:
		return "NORMAL"
	}
}

// handleIndex sirve la pagina principal del visualizador.
func (v *visualizer) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(web.IndexHTML))
}

// handleState devuelve el estado actual de todas las intersecciones.
func (v *visualizer) handleState(w http.ResponseWriter, _ *http.Request) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// Crear slice ordenado por fila y columna usando config
	list := make([]model.IntersectionSnapshot, 0, len(v.state))
	for _, intConfig := range v.cfg.Intersections {
		if snapshot, ok := v.state[intConfig.ID]; ok {
			list = append(list, snapshot)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

// handleEvents mantiene un stream SSE para actualizaciones en tiempo real.
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

// broadcast envia un evento generico a los suscriptores SSE.
func (v *visualizer) broadcast(payload []byte, topic string) {
	envelope := map[string]any{"topic": topic}

	data, _ := json.Marshal(envelope)
	for ch := range v.subs {
		select {
		case ch <- data:
		default:
		}
	}
}

// getenv lee una variable de entorno con fallback.
func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
