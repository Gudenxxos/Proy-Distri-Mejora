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

// runAuxiliaryMonitor levanta un servidor REP para analytics y tambien deja un CLI local disponible.
func runAuxiliaryMonitor(cfg config.CityConfig) {
	if cfg.Endpoints.MonitorAuxiliar == "" {
		log.Fatalf("[monitor-aux] MonitorAuxiliar endpoint no configurado")
	}

	log.Printf("[monitor-aux] iniciando modo auxiliar en endpoint: %s", cfg.Endpoints.MonitorAuxiliar)
	log.Printf("[monitor-aux] consultando desde DB Replica: %s", cfg.Endpoints.DBReplicaREP)

	router := storage.Router{
		Primary: newDBClient(cfg.Endpoints.DBReplicaREP),
		Replica: newDBClient(cfg.Endpoints.DBReplicaREP),
	}

	rep := listenAuxiliarySocket(cfg.Endpoints.MonitorAuxiliar)
	defer rep.Close()

	go serveAuxiliaryRequests(rep, &router)

	analyticsReq := dialAnalytics(cfg.Endpoints.AnalyticsREP)
	defer analyticsReq.Close()

	fmt.Println("[monitor-aux] listo. Escribe 'help' para ver comandos.")
	runConsoleLoop("[monitor-aux-cli]", &router, analyticsReq)
}

func listenAuxiliarySocket(endpoint string) zmq4.Socket {
	ctx := context.Background()

	var lastErr error
	for attempts := 1; attempts <= 3; attempts++ {
		rep := zmq4.NewRep(ctx)
		if err := rep.Listen(endpoint); err == nil {
			log.Printf("[monitor-aux] escuchando exitosamente en %s (intento %d)", endpoint, attempts)
			return rep
		} else {
			lastErr = err
			log.Printf("[monitor-aux] fallo al escuchar (intento %d): %v", attempts, err)
			rep.Close()
			if attempts < 3 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}

	log.Fatalf("[monitor-aux] no se pudo escuchar en %s: %v", endpoint, lastErr)
	return nil
}

func serveAuxiliaryRequests(rep zmq4.Socket, router *storage.Router) {
	log.Printf("[monitor-aux] esperando peticiones de analytics...")

	for {
		msg, err := rep.Recv()
		if err != nil {
			log.Printf("[monitor-aux] error recibiendo mensaje: %v", err)
			continue
		}

		response := handleAuxiliaryMessage(msg, router)
		data, _ := json.Marshal(response)
		_ = rep.Send(zmq4.NewMsg(data))
	}
}

func handleAuxiliaryMessage(msg zmq4.Msg, router *storage.Router) model.MonitorResponse {
	if len(msg.Frames) == 0 {
		return model.MonitorResponse{Success: false, Message: "empty request"}
	}

	var req model.MonitorRequest
	if err := json.Unmarshal(msg.Frames[0], &req); err != nil {
		log.Printf("[monitor-aux] error decodificando JSON: %v", err)
		return model.MonitorResponse{Success: false, Message: "invalid json"}
	}

	log.Printf("[monitor-aux] procesando accion: %s", req.Action)

	switch strings.ToLower(req.Action) {
	case "current":
		data, err := router.QueryCurrent(req.Intersection)
		if err != nil {
			return model.MonitorResponse{Success: false, Message: err.Error()}
		}

		var result []model.IntersectionSnapshot
		_ = json.Unmarshal(data, &result)
		return model.MonitorResponse{Success: true, Message: "consulta puntual", Data: result}
	case "history", "metric_count":
		body, _ := json.Marshal(req)
		data, err := router.QueryHistory(body)
		if err != nil {
			return model.MonitorResponse{Success: false, Message: err.Error()}
		}

		var result any
		_ = json.Unmarshal(data, &result)
		return model.MonitorResponse{Success: true, Message: "consulta procesada", Data: result}
	default:
		return model.MonitorResponse{Success: false, Message: "accion no soportada en monitor auxiliar"}
	}
}

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
			From:         time.Now().UTC().Add(-2 * time.Minute),
			To:           time.Now().UTC(),
			RequestedAt:  time.Now().UTC(),
		}
		executeMonitorRequest("[monitor]", &router, analyticsReq, req)
	}

	fmt.Println("[monitor-cli] Modo interactivo. Escribe 'help' para ver comandos.")
	runConsoleLoop("[monitor-cli]", &router, analyticsReq)
}

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

func parseConsoleCommand(line string) (model.MonitorRequest, bool, bool) {
	parts := strings.Fields(line)
	action := strings.ToLower(parts[0])

	req := model.MonitorRequest{
		Action:      action,
		From:        time.Now().UTC().Add(-2 * time.Minute),
		To:          time.Now().UTC(),
		RequestedAt: time.Now().UTC(),
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
		req.Intersection = parts[1]
		return req, false, true
	case "force_green":
		if len(parts) < 2 {
			fmt.Println("Uso: force_green <intersection> [duration_sec]")
			return req, false, false
		}
		req.Intersection = parts[1]
		req.DurationSec = 20
		if len(parts) >= 3 {
			duration, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Println("La duracion debe ser un numero entero de segundos")
				return req, false, false
			}
			req.DurationSec = duration
		}
		return req, false, true
	case "force_green_wave":
		if len(parts) < 2 {
			fmt.Println("Uso: force_green_wave <route>")
			return req, false, false
		}
		req.Route = parts[1]
		return req, false, true
	case "restore_automatic":
		if len(parts) < 2 {
			fmt.Println("Uso: restore_automatic <intersection>")
			return req, false, false
		}
		req.Intersection = parts[1]
		return req, false, true
	default:
		fmt.Printf("Comando no soportado: %s. Escribe 'help' para ver opciones.\n", parts[0])
		return req, false, false
	}
}

func executeMonitorRequest(prefix string, router *storage.Router, analyticsReq zmq4.Socket, req model.MonitorRequest) {
	switch strings.ToLower(req.Action) {
	case "current":
		data, err := router.QueryCurrent(req.Intersection)
		printResult(prefix, data, err)
	case "history", "metric_count":
		body, _ := json.Marshal(req)
		data, err := router.QueryHistory(body)
		printResult(prefix, data, err)
	default:
		sendAnalyticsRequest(prefix, analyticsReq, req)
	}
}

func printResult(prefix string, data []byte, err error) {
	if err != nil {
		fmt.Printf("%s error: %v\n", prefix, err)
		return
	}
	fmt.Printf("%s %s\n", prefix, data)
}

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

func printConsoleHelp() {
	fmt.Println(`Comandos disponibles:
  health
  current <intersection>
  history
  metric_count
  force_green <intersection> [duration_sec]
  force_green_wave <route>
  restore_automatic <intersection>
  exit | quit`)
}

func dialAnalytics(endpoint string) zmq4.Socket {
	socket := zmq4.NewReq(context.Background())
	if err := socket.Dial(endpoint); err != nil {
		socket.Close()
		log.Fatalf("dial analytics rep: %v", err)
	}
	return socket
}

type dbClient struct {
	endpoint string
}

func newDBClient(endpoint string) dbClient {
	return dbClient{endpoint: endpoint}
}

func (c dbClient) QueryCurrent(intersection string) ([]byte, error) {
	req := model.MonitorRequest{
		Action:       "current",
		Intersection: intersection,
		RequestedAt:  time.Now().UTC(),
	}
	return c.send(req)
}

func (c dbClient) QueryHistory(payload []byte) ([]byte, error) {
	var req model.MonitorRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	return c.send(req)
}

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

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
