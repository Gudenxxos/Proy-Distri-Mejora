package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
	"proy-distri/internal/storage"
)

// main arranca el servicio de semaforos y procesa comandos entrantes.
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
		timers:          make(map[string]*intersectionTimer),
		forceLocks:      make(map[string]time.Time),
		pushExecuted:    pushExecuted,
		pushVisualizer:  pushVisualizer,
	}

	// Inicializar estados de todos los semáforos
	for _, intersection := range cfg.Intersections {
		if intersection.HasSemaphore {
			app.states[intersection.ID] = model.LightPhaseHorizontal
			app.resetTimer(intersection.ID, 0)
		}
	}

	log.Printf("[traffic-light] esperando comandos en %s", cfg.Endpoints.TrafficLightPull)

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

// intersectionTimer controla reinicios y parada del temporizador por interseccion.
type intersectionTimer struct {
	resetCh chan int
	stopCh  chan struct{}
}

// trafficLightApp mantiene estado, temporizadores y canales de salida.
type trafficLightApp struct {
	cfg            config.CityConfig
	states         map[string]string
	stateMu        sync.RWMutex
	timers         map[string]*intersectionTimer
	timerMu        sync.Mutex
	forceLocks     map[string]time.Time
	forceLockMu    sync.Mutex
	sendMu         sync.Mutex
	pushExecuted   zmq4.Socket
	pushVisualizer zmq4.Socket
}

// processCommand valida y aplica un comando de luz recibido.
func (app *trafficLightApp) processCommand(cmd model.LightCommand) {
	intersection := normalizeIntersectionID(cmd.Intersection)
	if !app.hasSemaphore(intersection) {
		log.Printf("[traffic-light] ignorando comando para %s: no tiene semaforo", intersection)
		return
	}

	phase := strings.ToUpper(strings.TrimSpace(cmd.TargetState))
	if phase != model.LightPhaseVertical && phase != model.LightPhaseHorizontal {
		log.Printf("[traffic-light] ignorando comando invalido para %s: %s", intersection, cmd.TargetState)
		return
	}

	duration := cmd.DurationSec
	if duration <= 0 {
		duration = app.cfg.BaseGreenSeconds
	}

	now := storage.NowStoreTime()
	if lockedUntil, locked := app.activeForceLock(intersection, now); locked {
		log.Printf("[traffic-light] ignorando comando en %s: bloqueo force activo hasta %s", intersection, lockedUntil.Format(time.RFC3339))
		return
	}

	previous, err := app.setLightState(intersection, phase)
	if err != nil {
		log.Printf("[traffic-light] no se pudo aplicar comando en %s: %v", intersection, err)
		return
	}

	executed := cmd
	executed.Intersection = intersection
	executed.TargetState = phase
	executed.DurationSec = duration
	executed.ChangedAt = &now
	if executed.CommandID == "" {
		executed.CommandID = commandID("cmd", intersection, now)
	}
	if executed.RequestedAt.IsZero() {
		executed.RequestedAt = now
	}
	if executed.RequestedBy == "" {
		executed.RequestedBy = "analytics"
	}
	/* if executed.Reason == "" {
		executed.Reason = "manual_command"
	} */

	if isForceProtectedReason(executed.Reason) {
		app.setForceLock(intersection, now.Add(time.Duration(duration)*time.Second))
	}

	app.emitLightCommand(executed)
	log.Printf("[traffic-light] %s %s -> %s por %ds (reason=%s)", intersection, previous, phase, duration, executed.Reason)
	app.resetTimer(intersection, duration)
}

// activeForceLock devuelve el fin del bloqueo force si sigue vigente.
func (app *trafficLightApp) activeForceLock(intersection string, now time.Time) (time.Time, bool) {
	app.forceLockMu.Lock()
	defer app.forceLockMu.Unlock()

	until, ok := app.forceLocks[intersection]
	if !ok {
		return time.Time{}, false
	}

	if !now.Before(until) {
		delete(app.forceLocks, intersection)
		return time.Time{}, false
	}

	return until, true
}

// setForceLock fija el periodo en el que no se aceptan nuevos cambios.
func (app *trafficLightApp) setForceLock(intersection string, until time.Time) {
	app.forceLockMu.Lock()
	defer app.forceLockMu.Unlock()
	app.forceLocks[intersection] = until
}

// isForceProtectedReason identifica comandos que activan bloqueo temporal.
func isForceProtectedReason(reason string) bool {
	reason = strings.ToLower(strings.TrimSpace(reason))
	return reason == "force_green" || reason == "ola_verde"
}

// resetTimer rearma la duracion activa para una interseccion.
func (app *trafficLightApp) resetTimer(intersection string, durationSec int) {
	if durationSec <= 0 {
		durationSec = app.cfg.BaseGreenSeconds
	}

	timer := app.ensureTimer(intersection)
	select {
	case timer.resetCh <- durationSec:
	default:
		select {
		case <-timer.resetCh:
		default:
		}
		timer.resetCh <- durationSec
	}
}

// ensureTimer crea o recupera el timer dedicado de una interseccion.
func (app *trafficLightApp) ensureTimer(intersection string) *intersectionTimer {
	app.timerMu.Lock()
	defer app.timerMu.Unlock()

	if timer, ok := app.timers[intersection]; ok {
		return timer
	}

	timer := &intersectionTimer{
		resetCh: make(chan int, 1),
		stopCh:  make(chan struct{}),
	}
	app.timers[intersection] = timer
	go app.runTimerLoop(intersection, timer)
	return timer
}

// runTimerLoop ejecuta el ciclo automatico de alternancia por interseccion.
func (app *trafficLightApp) runTimerLoop(intersection string, timer *intersectionTimer) {
	var activeTimer *time.Timer
	defer func() {
		if activeTimer != nil && !activeTimer.Stop() {
			select {
			case <-activeTimer.C:
			default:
			}
		}
	}()

	for {
		var timerCh <-chan time.Time
		if activeTimer != nil {
			timerCh = activeTimer.C
		}

		select {
		case <-timer.stopCh:
			return
		case durationSec := <-timer.resetCh:
			if durationSec <= 0 {
				durationSec = app.cfg.BaseGreenSeconds
			}
			if activeTimer != nil && !activeTimer.Stop() {
				select {
				case <-activeTimer.C:
				default:
				}
			}
			activeTimer = time.NewTimer(time.Duration(durationSec) * time.Second)
			log.Printf("[traffic-light] %s temporizador ajustado a %ds", intersection, durationSec)
		case <-timerCh:
			previous, next, ok := app.flipLightState(intersection)
			if !ok {
				continue
			}

			now := storage.NowStoreTime()
			cmd := model.LightCommand{
				CommandID:    commandID("cycle", intersection, now),
				Intersection: intersection,
				TargetState:  next,
				DurationSec:  app.cfg.BaseGreenSeconds,
				Reason:       "cycle_end",
				RequestedBy:  "traffic_light",
				RequestedAt:  now,
				ChangedAt:    &now,
			}

			app.emitLightCommand(cmd)
			log.Printf("[traffic-light] %s ciclo %s -> %s por %ds (reason=cycle_end)", intersection, previous, next, app.cfg.BaseGreenSeconds)

			if activeTimer != nil {
				activeTimer.Reset(time.Duration(app.cfg.BaseGreenSeconds) * time.Second)
			}
		}
	}
}

// emitLightCommand publica el comando ejecutado a analytics y visualizer.
func (app *trafficLightApp) emitLightCommand(cmd model.LightCommand) {
	data, _ := json.Marshal(cmd)

	app.sendMu.Lock()
	defer app.sendMu.Unlock()

	if err := app.pushExecuted.Send(zmq4.NewMsg(data)); err != nil {
		log.Printf("[traffic-light] error enviando comando ejecutado: %v", err)
	}
	if err := app.pushVisualizer.Send(zmq4.NewMsg(data)); err != nil {
		log.Printf("[traffic-light] error enviando comando al visualizer: %v", err)
	}
}

// setLightState fija una fase concreta en memoria para una interseccion.
func (app *trafficLightApp) setLightState(intersection, phase string) (string, error) {
	phase = strings.ToUpper(strings.TrimSpace(phase))
	if phase != model.LightPhaseVertical && phase != model.LightPhaseHorizontal {
		return "", fmt.Errorf("invalid light phase %s", phase)
	}
	if !app.hasSemaphore(intersection) {
		return "", fmt.Errorf("intersection %s has no semaphore", intersection)
	}

	app.stateMu.Lock()
	defer app.stateMu.Unlock()

	previous := app.states[intersection]
	app.states[intersection] = phase
	return previous, nil
}

// flipLightState cambia a la fase opuesta cuando termina un ciclo.
func (app *trafficLightApp) flipLightState(intersection string) (string, string, bool) {
	if !app.hasSemaphore(intersection) {
		return "", "", false
	}

	app.stateMu.Lock()
	defer app.stateMu.Unlock()

	previous := app.states[intersection]
	next := model.OppositePhase(previous)
	if next == model.LightPhaseNone {
		return previous, "", false
	}

	app.states[intersection] = next
	return previous, next, true
}

// hasSemaphore valida si una interseccion admite cambios de luz.
func (app *trafficLightApp) hasSemaphore(intersection string) bool {
	intersection = normalizeIntersectionID(intersection)
	for _, item := range app.cfg.Intersections {
		if normalizeIntersectionID(item.ID) == intersection {
			return item.HasSemaphore
		}
	}
	return false
}

// normalizeIntersectionID normaliza IDs para comparaciones internas.
func normalizeIntersectionID(intersection string) string {
	return strings.ToUpper(strings.TrimSpace(intersection))
}

// commandID genera IDs trazables para comandos emitidos.
func commandID(prefix, intersection string, now time.Time) string {
	return fmt.Sprintf("%s-%s-%s", prefix, normalizeIntersectionID(intersection), now.Format("20060102150405.000000000"))
}

// getenv lee una variable de entorno con fallback.
func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
