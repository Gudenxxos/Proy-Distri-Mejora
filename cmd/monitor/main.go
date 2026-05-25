package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
	"proy-distri/internal/storage"
)

// main selecciona modo CLI o auxiliar segun variables de entorno.
func main() {
	cfgPath := getenv("CITY_CONFIG", "configs/city.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	auxEnvValue := strings.TrimSpace(getenv("AUX", "false"))
	isAux := strings.EqualFold(auxEnvValue, "true")

	log.Printf("[monitor] Iniciando con AUX=%s (modo=%s)", auxEnvValue, map[bool]string{true: "AUXILIAR", false: "CLI"}[isAux])

	if isAux {
		log.Printf("[monitor] Entrando en modo AUXILIAR")
		runAuxiliaryMonitor(cfg)
		return
	}

	log.Printf("[monitor] Entrando en modo CLI")
	runCLIMonitor(cfg)
}

// runAuxiliaryMonitor abre una consola local que consulta la replica y envia ordenes a analytics.
func runAuxiliaryMonitor(cfg config.CityConfig) {
	log.Printf("[monitor-aux] consultando desde DB Replica: %s", cfg.Endpoints.DBReplicaREP)

	router := storage.Router{
		Primary: newDBClient(cfg.Endpoints.DBReplicaREP),
		Replica: newDBClient(cfg.Endpoints.DBReplicaREP),
	}

	analyticsReq := dialAnalytics(cfg.Endpoints.AnalyticsREP)
	defer analyticsReq.Close()

	fmt.Println("[monitor-aux] listo. Escribe 'help' para ver comandos.")
	runConsoleLoop("[monitor-aux-cli]", &router, analyticsReq)
}

// runCLIMonitor ejecuta el monitor interactivo principal.
func runCLIMonitor(cfg config.CityConfig) {
	var (
		action       = flag.String("action", "", "Accion: health, current, history, force_green, force_green_wave, restore_automatic, metric_count")
		intersection = flag.String("intersection", "INT_B3", "Interseccion objetivo")
		route        = flag.String("route", "B", "Ruta priorizada")
		duration     = flag.Int("duration", 20, "Duracion en segundos")
	)
	flag.Parse()

	analyticsReq := dialAnalytics(cfg.Endpoints.AnalyticsREP)
	defer analyticsReq.Close()

	router := storage.Router{
		Primary: newDBClient(cfg.Endpoints.DBPrimaryREP),
		Replica: newDBClient(cfg.Endpoints.DBReplicaREP),
	}

	if *action != "" {
		req := model.MonitorRequest{
			Action:       *action,
			Intersection: *intersection,
			Route:        *route,
			DurationSec:  *duration,
			From:         storage.NowStoreTime().Add(-2 * time.Minute),
			To:           storage.NowStoreTime(),
			RequestedAt:  storage.NowStoreTime(),
		}
		executeMonitorRequest("[monitor]", &router, analyticsReq, req)
	}

	fmt.Println("[monitor-cli] Modo interactivo. Escribe 'help' para ver comandos.")
	runConsoleLoop("[monitor-cli]", &router, analyticsReq)
}

// runConsoleLoop procesa comandos de consola hasta salida explicita.
func runConsoleLoop(prefix string, router *storage.Router, analyticsReq zmq4.Socket) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("%s> ", prefix)
		if !scanner.Scan() {
			fmt.Println()
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		req, exit, ok := parseConsoleCommand(line)
		if exit {
			fmt.Printf("%s Saliendo...\n", prefix)
			return
		}
		if !ok {
			continue
		}

		executeMonitorRequest(prefix, router, analyticsReq, req)
	}
}

// parseConsoleCommand interpreta una linea de entrada a solicitud de monitor.
func parseConsoleCommand(line string) (model.MonitorRequest, bool, bool) {
	parts := strings.Fields(line)
	action := strings.ToLower(parts[0])

	req := model.MonitorRequest{
		Action:      action,
		From:        storage.NowStoreTime().Add(-2 * time.Minute),
		To:          storage.NowStoreTime(),
		RequestedAt: storage.NowStoreTime(),
	}

	switch action {
	case "help":
		printConsoleHelp()
		return req, false, false
	case "exit", "quit":
		return req, true, false
	case "health", "history", "metric_count":
		return req, false, true
	case "current":
		if len(parts) < 2 {
			fmt.Println("Uso: current <intersection>")
			return req, false, false
		}
		req.Intersection = strings.ToUpper(strings.TrimSpace(parts[1]))
		return req, false, true
	case model.ActionForceGreen:
		if len(parts) < 3 {
			fmt.Println("Uso: force_green <intersection> <duration_sec>")
			return req, false, false
		}
		duration, err := strconv.Atoi(parts[2])
		if err != nil || duration <= 0 {
			fmt.Println("Uso: force_green <intersection> <duration_sec>")
			return req, false, false
		}
		req.Intersection = strings.ToUpper(strings.TrimSpace(parts[1]))
		req.DurationSec = duration
		return req, false, true
	case "force_green_wave":
		if len(parts) < 2 {
			fmt.Println("Uso: force_green_wave <route>")
			return req, false, false
		}
		req.Route = strings.ToUpper(strings.TrimSpace(parts[1]))
		return req, false, true
	case "restore_automatic":
		if len(parts) < 2 {
			fmt.Println("Uso: restore_automatic <intersection>")
			return req, false, false
		}
		req.Intersection = strings.ToUpper(strings.TrimSpace(parts[1]))
		return req, false, true
	default:
		fmt.Printf("Comando no soportado: %s. Escribe 'help' para ver opciones.\n", parts[0])
		return req, false, false
	}
}

// executeMonitorRequest enruta consultas a BD o comandos a analytics.
func executeMonitorRequest(prefix string, router *storage.Router, analyticsReq zmq4.Socket, req model.MonitorRequest) {
	switch strings.ToLower(req.Action) {
	case "current":
		data, err := router.QueryCurrent(req.Intersection)
		printCurrentResult(prefix, data, err)
	case "history", "metric_count":
		body, _ := json.Marshal(req)
		data, err := router.QueryHistory(body)
		printResult(prefix, data, err)
	default:
		sendAnalyticsRequest(prefix, analyticsReq, req)
	}
}

// printResult imprime salida estandar de consultas.
func printResult(prefix string, data []byte, err error) {
	if err != nil {
		fmt.Printf("%s error: %v\n", prefix, err)
		return
	}
	fmt.Printf("%s %s\n", prefix, data)
}

// printCurrentResult muestra el snapshot actual en un bloque legible.
func printCurrentResult(prefix string, data []byte, err error) {
	if err != nil {
		fmt.Printf("%s error: %v\n", prefix, err)
		return
	}

	var snapshots []model.IntersectionSnapshot
	if err := json.Unmarshal(data, &snapshots); err != nil {
		fmt.Printf("%s error decodificando current: %v\n", prefix, err)
		return
	}

	if len(snapshots) == 0 {
		fmt.Printf("%s current: sin datos\n", prefix)
		return
	}

	for _, snapshot := range snapshots {
		fmt.Printf("%s current %s\n", prefix, snapshot.Intersection)
		fmt.Printf("%s   queue_length: %d\n", prefix, snapshot.QueueLength)
		fmt.Printf("%s   avg_speed: %.2f\n", prefix, snapshot.AvgSpeed)
		fmt.Printf("%s   density: %.2f\n", prefix, snapshot.Density)
		fmt.Printf("%s   vehicles_counted: %d\n", prefix, snapshot.VehiclesCounted)
		fmt.Printf("%s   light_state: %s\n", prefix, snapshot.LightState)
		fmt.Printf("%s   has_semaphore: %t\n", prefix, snapshot.HasSemaphore)
		fmt.Printf("%s   status: %s\n", prefix, snapshot.Status)
		fmt.Printf("%s   updated_at: %s\n", prefix, snapshot.UpdatedAt.Format(time.RFC3339))
	}
}

// sendAnalyticsRequest envia una solicitud REQ/REP al servicio analytics.
func sendAnalyticsRequest(prefix string, analyticsReq zmq4.Socket, req model.MonitorRequest) {
	body, _ := json.Marshal(req)
	if err := analyticsReq.Send(zmq4.NewMsg(body)); err != nil {
		fmt.Printf("%s error enviando a analytics: %v\n", prefix, err)
		return
	}

	msg, err := analyticsReq.Recv()
	if err != nil {
		fmt.Printf("%s error recibiendo de analytics: %v\n", prefix, err)
		return
	}

	if len(msg.Frames) > 0 {
		fmt.Printf("%s %s\n", prefix, msg.Frames[0])
	}
}

// printConsoleHelp muestra el listado resumido de comandos disponibles.
func printConsoleHelp() {
	fmt.Println(`Comandos disponibles:
  health
  current <intersection>
	force_green <intersection> <duration_sec>
  history
  metric_count
  force_green_wave <route>
  restore_automatic <intersection>
  exit | quit`)
}

// dialAnalytics crea el socket REQ contra el endpoint de analytics.
func dialAnalytics(endpoint string) zmq4.Socket {
	socket := zmq4.NewReq(context.Background())
	if err := socket.Dial(endpoint); err != nil {
		socket.Close()
		log.Fatalf("dial analytics rep: %v", err)
	}
	return socket
}

// dbClient encapsula acceso REQ/REP hacia db-server.
type dbClient struct {
	endpoint string
}

// newDBClient construye un cliente de base de datos para un endpoint.
func newDBClient(endpoint string) dbClient {
	return dbClient{endpoint: endpoint}
}

// QueryCurrent solicita el ultimo estado de una interseccion.
func (c dbClient) QueryCurrent(intersection string) ([]byte, error) {
	req := model.MonitorRequest{
		Action:       "current",
		Intersection: intersection,
		RequestedAt:  storage.NowStoreTime(),
	}
	return c.send(req)
}

// QueryHistory solicita historico de eventos para un rango temporal.
func (c dbClient) QueryHistory(payload []byte) ([]byte, error) {
	var req model.MonitorRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	return c.send(req)
}

// send ejecuta una llamada REQ/REP de bajo nivel contra db-server.
func (c dbClient) send(req model.MonitorRequest) ([]byte, error) {
	socket := zmq4.NewReq(context.Background())
	defer socket.Close()

	if err := socket.Dial(c.endpoint); err != nil {
		return nil, err
	}
	body, _ := json.Marshal(req)
	if err := socket.Send(zmq4.NewMsg(body)); err != nil {
		return nil, err
	}
	msg, err := socket.Recv()
	if err != nil {
		return nil, err
	}
	return msg.Frames[0], nil
}

// getenv lee una variable de entorno con fallback.
func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
