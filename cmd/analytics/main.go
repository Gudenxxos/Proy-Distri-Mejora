package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"

	analyticslogic "proy-distri/internal/analytics"
	"proy-distri/internal/config"
	"proy-distri/internal/model"
	"proy-distri/internal/storage"
)

// main inicializa analytics y orquesta el procesamiento central de eventos.
func main() {
	cfgPath := getenv("CITY_CONFIG", "configs/city.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	app := &analyticsApp{
		cfg:          cfg,
		evaluator:    analyticslogic.NewEvaluator(cfg),
		city:         model.NewCity(cfg),
		isPc3Healthy: true, // Inicialmente se asume que el DB primario está saludable
	}

	if err := app.run(); err != nil {
		log.Fatalf("analytics stopped: %v", err)
	}
}

// analyticsApp concentra estado, reglas y sockets de coordinacion.
type analyticsApp struct {
	cfg             config.CityConfig
	evaluator       analyticslogic.Evaluator
	city            *model.City
	mu              sync.Mutex
	sendMu          sync.Mutex
	isPc3Healthy    bool // Estado de salud del DB primario
	pc3HealthMutex  sync.RWMutex
	auxMonitorCmd   *os.Process // Proceso del monitor auxiliar
	auxMonitorMutex sync.Mutex  // Protege acceso a auxMonitorCmd
}

// run configura sockets, workers y bucle principal de consumo desde broker.
func (a *analyticsApp) run() error {
	ctx := context.Background()

	sub := zmq4.NewSub(ctx)
	if err := sub.Dial(a.cfg.Endpoints.BrokerFanout); err != nil {
		return err
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, "sensor."); err != nil {
		return err
	}

	pushLights := zmq4.NewPush(ctx)
	if err := pushLights.Dial(a.cfg.Endpoints.TrafficLightPull); err != nil {
		return err
	}

	pushPrimary := zmq4.NewPush(ctx)
	if err := pushPrimary.Dial(a.cfg.Endpoints.DBPrimaryPull); err != nil {
		return err
	}

	pushReplica := zmq4.NewPush(ctx)
	if err := pushReplica.Dial(a.cfg.Endpoints.DBReplicaPull); err != nil {
		return err
	}

	// Socket REP para recibir solicitudes de monitoreo
	rep := zmq4.NewRep(ctx)
	if err := rep.Listen(a.cfg.Endpoints.AnalyticsREP); err != nil {
		return err
	}

	// Socket PULL para recibir comandos ejecutados desde traffic-light
	pullExecutedLights := zmq4.NewPull(ctx)
	if err := pullExecutedLights.Listen(a.cfg.Endpoints.TrafficLightExecutedPush); err != nil {
		return err
	}

	// Socket REQ para health check con timeout configurado
	healthCheckReq := zmq4.NewReq(
		ctx,
		zmq4.WithTimeout(time.Duration(a.cfg.HealthCheckTimeoutMS)*time.Millisecond),
	)
	if err := healthCheckReq.Dial(a.cfg.Endpoints.DBPrimaryREP); err != nil {
		return err
	}
	defer healthCheckReq.Close()

	log.Printf("[analytics] suscrito a %s", a.cfg.Endpoints.BrokerFanout)
	log.Printf("[analytics] control REP en %s", a.cfg.Endpoints.AnalyticsREP)
	log.Printf("[analytics] health check configurado: intervalo=%dms, timeout=%dms",
		a.cfg.HealthCheckIntervalMS, a.cfg.HealthCheckTimeoutMS)

	// Goroutine para verificar periódicamente la salud del DB primario
	go a.healthCheckLoop(ctx)

	go a.handleRequests(rep, pushLights)

	// Goroutine para recibir y persistir comandos ejecutados desde traffic-light
	go a.handleExecutedLightCommands(pullExecutedLights, pushPrimary, pushReplica)

	for {
		msg, err := sub.Recv()
		if err != nil {
			log.Printf("analytics sub recv: %v", err)
			continue
		}
		if len(msg.Frames) < 2 {
			continue
		}

		topic := string(msg.Frames[0])
		payload := msg.Frames[1]
		a.processSensor(topic, payload, pushLights, pushPrimary, pushReplica)
	}
}

// healthCheckLoop verifica periódicamente la salud del DB primario usando REQ/REP con timeout.
// Detecta fallos tipo Crash o Fail-noisy actualizando isPc3Healthy.
func (a *analyticsApp) healthCheckLoop(ctx context.Context) {

	ticker := time.NewTicker(time.Duration(a.cfg.HealthCheckIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {

		// Cada ciclo, se crea un nuevo socket REQ para evitar problemas de estado interno tras timeouts o errores.
		req := zmq4.NewReq(
			ctx,
			zmq4.WithTimeout(time.Duration(a.cfg.HealthCheckTimeoutMS)*time.Millisecond),
		)

		if err := req.Dial(a.cfg.Endpoints.DBPrimaryREP); err != nil {
			req.Close()
			a.setPC3HealthStatus(false, "error conectando health check: %v", err)
			continue
		}

		healthReq := model.MonitorRequest{
			Action:      "health",
			RequestedAt: storage.NowStoreTime(),
		}
		data, _ := json.Marshal(healthReq)

		if err := req.Send(zmq4.NewMsg(data)); err != nil {
			req.Close()
			a.setPC3HealthStatus(false, "error al enviar health check: %v", err)
			continue
		}

		msg, err := req.Recv()
		req.Close()

		if err != nil {
			a.setPC3HealthStatus(false, "DB primario no responde: %v", err)
			continue
		}

		var resp model.MonitorResponse
		if len(msg.Frames) == 0 || json.Unmarshal(msg.Frames[0], &resp) != nil || !resp.Success {
			a.setPC3HealthStatus(false, "respuesta invalida del DB primario")
			continue
		}

		a.setPC3HealthStatus(true, "DB primario saludable")
	}
}

// setPC3HealthStatus actualiza el estado de salud y registra cambios de estado.
// Si el estado cambia a unhealthy, lanza el monitor auxiliar en PC2.
// Si el estado cambia a healthy, termina el monitor auxiliar.
func (a *analyticsApp) setPC3HealthStatus(healthy bool, format string, args ...interface{}) {
	a.pc3HealthMutex.Lock()
	defer a.pc3HealthMutex.Unlock()

	// Si el estado cambió, registrar el evento
	if a.isPc3Healthy != healthy {
		statusStr := "SALUDABLE"
		if !healthy {
			statusStr = "NO SALUDABLE"
		}
		log.Printf("[analytics] CAMBIO DE ESTADO: DB primario %s - "+format,
			append([]interface{}{statusStr}, args...)...)
		a.isPc3Healthy = healthy

		if !healthy {
			// Circuit breaker abierto: lanzar monitor auxiliar
			a.startAuxMonitor()
		} else {
			// Circuit breaker cerrado: terminar monitor auxiliar
			a.stopAuxMonitor()
		}
	}
}

// startAuxMonitor lanza el proceso de Monitor en modo auxiliar (AUX=true) en una terminal separada
func (a *analyticsApp) startAuxMonitor() {
	a.auxMonitorMutex.Lock()
	defer a.auxMonitorMutex.Unlock()

	if a.auxMonitorCmd != nil {
		// Ya existe, no iniciar otro
		return
	}

	log.Printf("[analytics] Iniciando Monitor Auxiliar en PC2 (en terminal separada)...")

	var cmd *exec.Cmd

	if runtime.GOOS == "windows" {
		// Windows: abrir una terminal nueva y definir AUX dentro de esa terminal.
		cmd = exec.Command(
			"cmd.exe",
			"/c",
			"start",
			"Monitor Auxiliar",
			"powershell.exe",
			"-NoProfile",
			"-NoExit",
			"-Command",
			`$env:AUX='true'; $env:CITY_CONFIG='configs\city.json'; & '.\monitor.exe'`,
		)
	} else {
		// Linux/macOS: Lanzar en segundo plano con AUX=true
		cmd = exec.Command(
			"sh",
			"-c",
			"AUX=true ./monitor &",
		)
	}

	if err := cmd.Start(); err != nil {
		log.Printf("[analytics] Error iniciando Monitor Auxiliar: %v", err)
		return
	}

	if runtime.GOOS == "windows" {
		log.Printf("[analytics] Monitor Auxiliar lanzado en terminal separada")
		return
	}

	a.auxMonitorCmd = cmd.Process
	log.Printf("[analytics] Monitor Auxiliar iniciado (PID: %d)", a.auxMonitorCmd.Pid)

	// Monitorear el proceso en segundo plano
	go func() {
		exitErr := cmd.Wait()
		a.auxMonitorMutex.Lock()
		a.auxMonitorCmd = nil
		a.auxMonitorMutex.Unlock()
		if exitErr != nil {
			log.Printf("[analytics] Monitor Auxiliar terminó con error: %v", exitErr)
		} else {
			log.Printf("[analytics] Monitor Auxiliar terminó")
		}
	}()
}

// stopAuxMonitor termina el proceso del monitor auxiliar
func (a *analyticsApp) stopAuxMonitor() {
	a.auxMonitorMutex.Lock()
	defer a.auxMonitorMutex.Unlock()

	if a.auxMonitorCmd == nil {
		return
	}

	log.Printf("[analytics] Terminando Monitor Auxiliar (PID: %d)...", a.auxMonitorCmd.Pid)
	if err := a.auxMonitorCmd.Kill(); err != nil {
		log.Printf("[analytics] Error terminando Monitor Auxiliar: %v", err)
	}
	a.auxMonitorCmd = nil
}

// handleExecutedLightCommands escucha comandos ejecutados desde traffic-light
// y los persiste en las bases de datos primary y replica
func (a *analyticsApp) handleExecutedLightCommands(pullExecuted, pushPrimary, pushReplica zmq4.Socket) {
	for {
		msg, err := pullExecuted.Recv()
		if err != nil {
			log.Printf("[analytics] error recibiendo comando ejecutado: %v", err)
			continue
		}
		if len(msg.Frames) == 0 {
			continue
		}

		var cmd model.LightCommand
		if err := json.Unmarshal(msg.Frames[0], &cmd); err != nil {
			log.Printf("[analytics] error decodificando comando ejecutado: %v", err)
			continue
		}

		if cmd.ChangedAt == nil {
			log.Printf("[analytics] comando ejecutado sin ChangedAt: %s", cmd.CommandID)
			continue
		}

		log.Printf("[analytics] recibido comando ejecutado: %s (requested=%v, changed=%v)",
			cmd.CommandID, cmd.RequestedAt, cmd.ChangedAt)

		/* AQUÍ DEBERÍA CAMBIAR EL OBJETO DE CIUDAD PARA REFLEJAR EL CAMBIO DE ESTADO */
		a.mu.Lock()
		a.city.SetLight(cmd.Intersection, cmd.TargetState)
		current, currentExists := a.city.Get(cmd.Intersection)
		a.mu.Unlock()

		// Persistir comando ejecutado
		data, _ := json.Marshal(cmd)
		env := model.PersistEnvelope{
			Kind:         "light_command_executed",
			Topic:        model.TopicCommand,
			RawPayload:   string(data),
			LightCommand: &cmd,
			CreatedAt:    *cmd.ChangedAt,
		}

		snpshot := &model.IntersectionSnapshot{
			Intersection: cmd.Intersection,
			LightState:   cmd.TargetState,
			UpdatedAt:    *cmd.ChangedAt,
		}
		if currentExists && current != nil {
			snpshot.QueueLength = current.QueueLength
			snpshot.AvgSpeed = current.AvgSpeed
			snpshot.Density = current.Density
			snpshot.VehiclesCounted = current.VehiclesCount
			snpshot.HasSemaphore = current.HasSemaphore
			snpshot.Status = current.Status
		}
		env.Snapshot = snpshot
		a.persistEnvelope(env, pushPrimary, pushReplica)
		a.persistSnapshot(*snpshot, "solved.state", data, pushPrimary, pushReplica)
	}
}

// IsPC3Healthy retorna el estado actual del DB primario de manera thread-safe.
func (a *analyticsApp) IsPC3Healthy() bool {
	a.pc3HealthMutex.RLock()
	defer a.pc3HealthMutex.RUnlock()
	return a.isPc3Healthy
}

// processSensor aplica un evento de sensor sobre el modelo y decide acciones.
func (a *analyticsApp) processSensor(topic string, payload []byte, pushLights, pushPrimary, pushReplica zmq4.Socket) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var (
		snapshot *model.IntersectionSnapshot
		err      error
	)

	switch topic {
	case model.TopicCamera:
		var event model.CameraEvent
		if err = json.Unmarshal(payload, &event); err == nil {
			snapshot, err = a.city.UpdateFromCamera(event.Interseccion, event.Volumen, event.VelocidadPromedio)
		}
	case model.TopicGPS:
		var event model.GPSEvent
		if err = json.Unmarshal(payload, &event); err == nil {
			snapshot, err = a.city.UpdateFromGPS(event.Interseccion, event.Densidad, event.VelocidadPromedio, event.NivelCongestion)
		}
	case model.TopicInductive:
		var event model.InductiveEvent
		if err = json.Unmarshal(payload, &event); err == nil {
			snapshot, err = a.city.UpdateFromInductive(event.Interseccion, event.VehiculosContados)
		}
	}

	if err != nil || snapshot == nil {
		log.Printf("[analytics] process %s: %v", topic, err)
		return
	}

	a.city.ApplyInfluence(snapshot.Intersection)
	current, _ := a.city.Get(snapshot.Intersection)
	latest := &model.IntersectionSnapshot{
		Intersection:    current.ID,
		QueueLength:     current.QueueLength,
		AvgSpeed:        current.AvgSpeed,
		Density:         current.Density,
		VehiclesCounted: current.VehiclesCount,
		LightState:      current.LightState,
		HasSemaphore:    current.HasSemaphore,
		Status:          current.Status,
		UpdatedAt:       current.LastUpdate,
	}

	status, command := a.evaluator.Evaluate(*latest)
	latest.Status = status
	a.city.SetStatus(latest.Intersection, status)

	a.persistSnapshot(*latest, topic, payload, pushPrimary, pushReplica)

	if command != nil && latest.HasSemaphore {
		log.Printf("[analytics] %s => %s (%s)", latest.Intersection, command.TargetState, command.Reason)
		a.sendLightCommand(*command, pushLights)
	}
}

/* Función para escuchar solicitudes de monitoreo */
func (a *analyticsApp) handleRequests(rep, pushLights zmq4.Socket) {
	for {
		msg, err := rep.Recv()
		if err != nil {
			log.Printf("analytics req recv: %v", err)
			return
		}

		var req model.MonitorRequest
		if len(msg.Frames) == 0 {
			_ = rep.Send(zmq4.NewMsgString(`{"success":false,"message":"empty request"}`))
			continue
		}

		if err := json.Unmarshal(msg.Frames[0], &req); err != nil {
			_ = rep.Send(zmq4.NewMsgString(`{"success":false,"message":"invalid json"}`))
			continue
		}

		response := a.handleRequest(req, pushLights)
		data, _ := json.Marshal(response)
		_ = rep.Send(zmq4.NewMsg(data))
	}
}

/* Función para ejecutar solicitudes de monitoreo */
func (a *analyticsApp) handleRequest(req model.MonitorRequest, pushLights zmq4.Socket) model.MonitorResponse {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch strings.ToLower(req.Action) {
	case "health":
		return model.MonitorResponse{Success: true, Message: "analytics ok"}
	case model.ActionForceGreen:
		currentPhase, ok := a.city.Get(req.Intersection)
		if !ok {
			return model.MonitorResponse{Success: false, Message: "intersection not found"}
		}
		cmd, err := a.evaluator.BuildForceGreen(req.Intersection, req.DurationSec, currentPhase.LightState)
		if err != nil {
			return model.MonitorResponse{Success: false, Message: err.Error()}
		}
		log.Printf("[analytics] force_green %s -> %s por %ds", cmd.Intersection, cmd.TargetState, cmd.DurationSec)
		a.sendLightCommand(cmd, pushLights)
		return model.MonitorResponse{Success: true, Message: "force_green aplicado", Data: cmd}
	case "force_green_wave":
		commands := a.evaluator.BuildPriorityWave(req.Route, a.city)
		for _, cmd := range commands {
			a.sendLightCommand(cmd, pushLights)
		}
		return model.MonitorResponse{Success: true, Message: "ola verde aplicada", Data: commands}
	case "restore_automatic":
		intersection := strings.ToUpper(strings.TrimSpace(req.Intersection))
		if current, ok := a.city.Get(intersection); !ok || !current.HasSemaphore {
			return model.MonitorResponse{Success: false, Message: "intersection has no semaphore"}
		}
		targetPhase := model.PreferredPhaseForIntersectionID(intersection)
		cmd := model.LightCommand{
			CommandID:    commandID(),
			Intersection: intersection,
			TargetState:  targetPhase,
			DurationSec:  a.cfg.BaseGreenSeconds,
			Reason:       analyticslogic.ReasonNormal,
			RequestedBy:  analyticslogic.RequestMonitoring,
			RequestedAt:  storage.NowStoreTime(),
		}
		log.Printf("[analytics] restore_automatic %s -> %s por %ds", cmd.Intersection, cmd.TargetState, cmd.DurationSec)
		a.sendLightCommand(cmd, pushLights)
		return model.MonitorResponse{Success: true, Message: "modo automatico restaurado", Data: cmd}
	default:
		return model.MonitorResponse{Success: false, Message: "accion no soportada"}
	}
}

// sendLightCommand publica comandos hacia traffic-light con serializacion segura.
func (a *analyticsApp) sendLightCommand(cmd model.LightCommand, pushLights zmq4.Socket) {
	// Asignar RequestedAt y dejar ChangedAt vacío
	cmd.RequestedAt = storage.NowStoreTime()
	cmd.ChangedAt = nil

	data, _ := json.Marshal(cmd)
	a.sendMu.Lock()
	_ = pushLights.Send(zmq4.NewMsg(data))
	a.sendMu.Unlock()
}

// persistSnapshot adapta un snapshot a sobre de persistencia.
func (a *analyticsApp) persistSnapshot(snapshot model.IntersectionSnapshot, topic string, raw []byte, pushPrimary, pushReplica zmq4.Socket) {
	env := model.PersistEnvelope{
		Kind:       "snapshot",
		Topic:      topic,
		RawPayload: string(raw),
		Snapshot:   &snapshot,
		CreatedAt:  storage.NowStoreTime(),
	}
	a.persistEnvelope(env, pushPrimary, pushReplica)
}

// persistEnvelope aplica politica de circuit breaker para persistencia primaria.
func (a *analyticsApp) persistEnvelope(env model.PersistEnvelope, pushPrimary, pushReplica zmq4.Socket) {
	if env.EventID == "" {
		env.EventID = eventID()
	}

	data, _ := json.Marshal(env)
	a.sendMu.Lock()
	defer a.sendMu.Unlock()

	// Circuit Breaker: si el primary está caído, solo enviar a replica
	if a.IsPC3Healthy() {
		// Primary está saludable: enviar a ambos
		if err := pushPrimary.Send(zmq4.NewMsg(data)); err != nil {
			log.Printf("[analytics] Error enviando a primary: %v", err)
		}
	} else {
		// Primary está caído (circuit breaker abierto): enviar solo a replica
		log.Printf("[analytics] Circuit Breaker ABIERTO - redirigiendo persistencia a replica")
	}

	// Siempre enviar a replica (para sincronización)
	if err := pushReplica.Send(zmq4.NewMsg(data)); err != nil {
		log.Printf("[analytics] Error enviando a replica: %v", err)
	}
}

// commandID genera IDs de comando ordenables por tiempo.
func commandID() string {
	return "cmd-" + storage.NowStoreTime().Format("20060102150405.000000000")
}

// eventID genera el identificador comun de cada evento persistido.
func eventID() string {
	return "evt-" + storage.NowStoreTime().Format("20060102150405.000000000")
}

// getenv lee una variable de entorno con fallback.
func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
