package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
	"proy-distri/internal/storage"
)

func main() {
	cfgPath := getenv("CITY_CONFIG", "configs/city.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx := context.Background()
	pull := zmq4.NewPull(ctx)
	defer pull.Close()
	if err := pull.Listen(cfg.Endpoints.TrafficLightPull); err != nil {
		log.Fatalf("listen traffic light pull: %v", err)
	}

	// PUSH para enviar comandos ejecutados de vuelta a analytics
	pushExecuted := zmq4.NewPush(ctx)
	defer pushExecuted.Close()
	if err := pushExecuted.Dial(cfg.Endpoints.TrafficLightExecutedPush); err != nil {
		log.Fatalf("dial traffic light executed push: %v", err)
	}

	// PUSH para enviar cambios de semáforos al visualizer
	pushVisualizer := zmq4.NewPush(ctx)
	defer pushVisualizer.Close()
	if err := pushVisualizer.Dial(cfg.Endpoints.VisualizerLightPush); err != nil {
		log.Fatalf("dial visualizer light push: %v", err)
	}

	app := &trafficLightApp{
		cfg:             cfg,
		states:          make(map[string]string),
		pushExecuted:    pushExecuted,
		pushVisualizer:  pushVisualizer,
		globalCycleCh:   make(chan *globalCycleUpdate, 10),
		stopGlobalTimer: make(chan struct{}),
	}

	// Inicializar estados de todos los semáforos
	for _, intersection := range cfg.Intersections {
		if intersection.HasSemaphore {
			app.states[intersection.ID] = model.LightPhaseNone
		}
	}

	log.Printf("[traffic-light] esperando comandos en %s", cfg.Endpoints.TrafficLightPull)

	// Iniciar goroutine del timer global
	go app.globalTimerLoop()

	// Bucle principal de recepción de comandos
	for {
		msg, err := pull.Recv()
		if err != nil {
			log.Fatalf("recv command: %v", err)
		}
		if len(msg.Frames) == 0 {
			continue
		}

		var cmd model.LightCommand
		if err := json.Unmarshal(msg.Frames[0], &cmd); err != nil {
			log.Printf("decode command: %v", err)
			continue
		}

		app.processCommand(cmd)
	}
}

type globalCycleUpdate struct {
	durationSec int
	phaseEnd    time.Time
}

type trafficLightApp struct {
	cfg            config.CityConfig
	states         map[string]string
	mu             sync.RWMutex
	pushExecuted   zmq4.Socket
	pushVisualizer zmq4.Socket

	// Global timer management
	globalCycleCh   chan *globalCycleUpdate
	stopGlobalTimer chan struct{}
	globalEndTime   time.Time
	globalMutex     sync.RWMutex
}

func (app *trafficLightApp) processCommand(cmd model.LightCommand) {
	now := storage.NowStoreTime()

	app.mu.Lock()
	defer app.mu.Unlock()

	// Actualizar TODAS las intersecciones con semáforo
	for _, intersection := range app.cfg.Intersections {
		if !intersection.HasSemaphore {
			continue
		}

		// Cambiar estado interno
		app.states[intersection.ID] = cmd.TargetState

		// Crear comando específico para esta intersección
		updatedCmd := model.LightCommand{
			CommandID:    cmd.CommandID,
			Intersection: intersection.ID,
			TargetState:  cmd.TargetState,
			DurationSec:  cmd.DurationSec,
			Reason:       cmd.Reason,
			RequestedBy:  cmd.RequestedBy,
			RequestedAt:  cmd.RequestedAt,
			ChangedAt:    &now,
		}

		// Serializar
		cmdData, _ := json.Marshal(updatedCmd)

		// Enviar a analytics
		_ = app.pushExecuted.Send(zmq4.NewMsg(cmdData))

		// Enviar al visualizer
		_ = app.pushVisualizer.Send(zmq4.NewMsg(cmdData))

		log.Printf(
			"[traffic-light] %s -> %s por %s durante %ds",
			intersection.ID,
			cmd.TargetState,
			cmd.Reason,
			cmd.DurationSec,
		)
	}

	// Actualizar ciclo global
	app.globalCycleCh <- &globalCycleUpdate{
		durationSec: cmd.DurationSec,
		phaseEnd:    now.Add(time.Duration(cmd.DurationSec) * time.Second),
	}
}

// globalTimerLoop maneja un único timer global para todos los semáforos.
// Cuando expira el ciclo, cambia todos los semáforos al estado opuesto.
func (app *trafficLightApp) globalTimerLoop() {
	ticker := time.NewTicker(100 * time.Millisecond) // Verificar cada 100ms
	defer ticker.Stop()

	var (
		globalDuration = time.Duration(app.cfg.BaseGreenSeconds) * time.Second
		globalEndTime  = time.Now().Add(globalDuration)
	)

	for {
		select {
		case <-app.stopGlobalTimer:
			return
		case update := <-app.globalCycleCh:
			// Actualizar el tiempo final del ciclo global
			globalEndTime = update.phaseEnd
			globalDuration = time.Duration(update.durationSec) * time.Second
			log.Printf("[traffic-light] Ciclo global actualizado: %ds", update.durationSec)
		case <-ticker.C:
			// Verificar si el ciclo global ha expirado
			if time.Now().After(globalEndTime) {
				app.cycleGlobalLights()
				// Reiniciar con duración base
				globalDuration = time.Duration(app.cfg.BaseGreenSeconds) * time.Second
				globalEndTime = time.Now().Add(globalDuration)
			}
		}
	}
}

// cycleGlobalLights cambia todos los semáforos al estado opuesto cuando expira el ciclo.
func (app *trafficLightApp) cycleGlobalLights() {
	app.mu.Lock()
	defer app.mu.Unlock()

	now := storage.NowStoreTime()
	for intersectionID, currentPhase := range app.states {
		nextPhase := model.OppositePhase(currentPhase)
		if nextPhase == "" {
			continue // No cambiar si es NONE
		}

		app.states[intersectionID] = nextPhase

		// Crear evento de cambio de ciclo
		cmd := model.LightCommand{
			CommandID:    "cycle-" + now.Format("20060102150405.000000000"),
			Intersection: intersectionID,
			TargetState:  nextPhase,
			Reason:       "cycle_end",
			RequestedBy:  "traffic_light",
			RequestedAt:  now,
			ChangedAt:    &now,
		}

		// Enviar al visualizer
		cmdData, _ := json.Marshal(cmd)
		_ = app.pushVisualizer.Send(zmq4.NewMsg(cmdData))

		log.Printf("[traffic-light] %s ciclo auto: %s -> %s", intersectionID, currentPhase, nextPhase)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
