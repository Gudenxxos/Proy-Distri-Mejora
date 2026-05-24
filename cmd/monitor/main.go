package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
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

	// Verificar si se ejecuta en modo auxiliar
	auxEnvValue := strings.TrimSpace(getenv("AUX", "false"))
	isAux := strings.EqualFold(auxEnvValue, "true")

	log.Printf("[monitor] Iniciando con AUX=%s (modo=%s)", auxEnvValue, map[bool]string{true: "AUXILIAR", false: "CLI"}[isAux])

	if isAux {
		// Modo Auxiliar: Levantarse como servidor REP que consulta la replica
		log.Printf("[monitor] Entrando en modo AUXILIAR")
		runAuxiliaryMonitor(cfg)
	} else {
		// Modo CLI: Comportamiento original
		log.Printf("[monitor] Entrando en modo CLI")
		runCLIMonitor(cfg)
	}
}

// runAuxiliaryMonitor levanta el monitor en modo servidor auxiliar
// Escucha en MonitorAuxiliar Y mantiene un CLI interactivo
func runAuxiliaryMonitor(cfg config.CityConfig) {
	if cfg.Endpoints.MonitorAuxiliar == "" {
		log.Fatalf("[monitor-aux] MonitorAuxiliar endpoint no configurado")
	}

	log.Printf("[monitor-aux] iniciando modo auxiliar en endpoint: %s", cfg.Endpoints.MonitorAuxiliar)
	log.Printf("[monitor-aux] consultando desde DB Replica: %s", cfg.Endpoints.DBReplicaREP)

	ctx := context.Background()

	// Router que consulta solo la réplica
	router := storage.Router{
		Primary: newDBClient(cfg.Endpoints.DBReplicaREP), // Usar replica como primary
		Replica: newDBClient(cfg.Endpoints.DBReplicaREP),
	}

	// Inicializar socket REP y escuchar en goroutine
	var rep zmq4.Socket
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		rep = zmq4.NewRep(ctx)
		if err := rep.Listen(cfg.Endpoints.MonitorAuxiliar); err == nil {
			log.Printf("[monitor-aux] escuchando exitosamente en %s (intento %d)", cfg.Endpoints.MonitorAuxiliar, attempts+1)
			break
		} else {
			log.Printf("[monitor-aux] fallo al escuchar (intento %d): %v", attempts+1, err)
			rep.Close()
			if attempts < 2 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
	if err != nil {
		log.Fatalf("[monitor-aux] no se pudo escuchar en %s después de reintentos", cfg.Endpoints.MonitorAuxiliar)
	}
	defer rep.Close()

	log.Printf("[monitor-aux] iniciado correctamente, esperando peticiones en background...")
	log.Printf("[monitor-aux] CLI también disponible (escribe comandos o 'help' para ver opciones)\n")

	// Lanzar servidor REP en goroutine
	go func() {
		for {
			msg, err := rep.Recv()
			if err != nil {
				log.Printf("[monitor-aux] error recibiendo mensaje: %v", err)
				continue
			}

			if len(msg.Frames) == 0 {
				_ = rep.Send(zmq4.NewMsgString(`{"success":false,"message":"empty request"}`))
				continue
			}

			var req model.MonitorRequest
			if err := json.Unmarshal(msg.Frames[0], &req); err != nil {
				log.Printf("[monitor-aux] error decodificando JSON: %v", err)
				_ = rep.Send(zmq4.NewMsgString(`{"success":false,"message":"invalid json"}`))
				continue
			}

			log.Printf("[monitor-aux] procesando acción: %s", req.Action)

			// Procesar acción
			var response model.MonitorResponse

			switch strings.ToLower(req.Action) {
			case "current":
				data, err := router.QueryCurrent(req.Intersection)
				if err != nil {
					log.Printf("[monitor-aux] error en consulta current: %v", err)
					response = model.MonitorResponse{Success: false, Message: fmt.Sprintf("error: %v", err)}
				} else {
					var result []model.IntersectionSnapshot
					json.Unmarshal(data, &result)
					response = model.MonitorResponse{Success: true, Message: "consulta puntual", Data: result}
				}
			case "history", "metric_count":
				body, _ := json.Marshal(req)
				data, err := router.QueryHistory(body)
				if err != nil {
					log.Printf("[monitor-aux] error en consulta history/metric: %v", err)
					response = model.MonitorResponse{Success: false, Message: fmt.Sprintf("error: %v", err)}
				} else {
					var result interface{}
					json.Unmarshal(data, &result)
					response = model.MonitorResponse{Success: true, Message: "consulta procesada", Data: result}
				}
			default:
				log.Printf("[monitor-aux] acción no soportada: %s", req.Action)
				response = model.MonitorResponse{Success: false, Message: "accion no soportada en monitor auxiliar"}
			}

			responseData, _ := json.Marshal(response)
			_ = rep.Send(zmq4.NewMsg(responseData))
		}
	}()

	// CLI Interactivo en el main thread (igual que runCLIMonitor)
	runInteractiveCLI(cfg, router)
}

// runInteractiveCLI ejecuta un CLI interactivo que lee comandos desde stdin
func runInteractiveCLI(cfg config.CityConfig, router storage.Router) {
	ctx := context.Background()
	analyticsReq := zmq4.NewReq(ctx)
	defer analyticsReq.Close()
	if err := analyticsReq.Dial(cfg.Endpoints.AnalyticsREP); err != nil {
		log.Fatalf("dial analytics rep: %v", err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("\n[monitor-aux-cli]> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		action := parts[0]

		switch strings.ToLower(action) {
		case "help":
			fmt.Println(`
Comandos disponibles:
  current <intersection>          - Ver estado actual de una intersección
  history <intersection>           - Ver histórico de una intersección
  metric_count <intersection>      - Contar eventos en rango de tiempo
  force_green <intersection>       - Forzar luz verde
  force_green_wave <route>         - Forzar onda verde en ruta
  restore_automatic <intersection> - Restaurar control automático
  exit, quit                       - Salir
  help                            - Ver este mensaje
			`)

		case "exit", "quit":
			fmt.Println("[monitor-aux-cli] Saliendo...")
			return

		case "current":
			if len(parts) < 2 {
				fmt.Println("Uso: current <intersection>")
				continue
			}
			intersection := parts[1]
			data, err := router.QueryCurrent(intersection)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("[monitor-aux-cli] %s\n", data)
			}

		case "history", "metric_count":
			if len(parts) < 2 {
				fmt.Printf("Uso: %s <intersection>\n", action)
				continue
			}
			intersection := parts[1]
			req := model.MonitorRequest{
				Action:       action,
				Intersection: intersection,
				From:         time.Now().UTC().Add(-2 * time.Minute),
				To:           time.Now().UTC(),
				RequestedAt:  time.Now().UTC(),
			}
			body, _ := json.Marshal(req)
			data, err := router.QueryHistory(body)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("[monitor-aux-cli] %s\n", data)
			}

		default:
			// Enviar comando a analytics
			req := model.MonitorRequest{
				Action:       action,
				Intersection: parts[len(parts)-1],
				RequestedAt:  time.Now().UTC(),
			}
			body, _ := json.Marshal(req)
			if err := analyticsReq.Send(zmq4.NewMsg(body)); err != nil {
				fmt.Printf("Error enviando a analytics: %v\n", err)
			} else {
				msg, err := analyticsReq.Recv()
				if err != nil {
					fmt.Printf("Error recibiendo de analytics: %v\n", err)
				} else if len(msg.Frames) > 0 {
					fmt.Printf("[monitor-aux-cli] %s\n", msg.Frames[0])
				}
			}
		}
	}
}

// runCLIMonitor ejecuta el monitor en modo CLI interactivo
func runCLIMonitor(cfg config.CityConfig) {
	ctx := context.Background()
	analyticsReq := zmq4.NewReq(ctx)
	defer analyticsReq.Close()
	if err := analyticsReq.Dial(cfg.Endpoints.AnalyticsREP); err != nil {
		log.Fatalf("dial analytics rep: %v", err)
	}

	router := storage.Router{
		Primary: newDBClient(cfg.Endpoints.DBPrimaryREP),
		Replica: newDBClient(cfg.Endpoints.DBReplicaREP),
	}

	// Parsear flags para ejecución única (opcional)
	var (
		action       = flag.String("action", "", "Accion: health, current, history, force_green, force_green_wave, restore_automatic, metric_count")
		intersection = flag.String("intersection", "INT_B3", "Interseccion objetivo")
		route        = flag.String("route", "B", "Ruta priorizada")
		duration     = flag.Int("duration", 20, "Duracion en segundos")
	)
	flag.Parse()

	// Si se proporciona acción por flag, ejecutarla y luego pasar a CLI interactivo
	if *action != "" {
		executeMonitorAction(*action, *intersection, *route, *duration, &router, analyticsReq)
	}

	// CLI Interactivo
	fmt.Println("\n[monitor-cli] Entrando en modo interactivo. Escribe 'help' para ver comandos.")
	fmt.Println("[monitor-cli] Escribe 'exit' para salir.")

	runMonitorInteractiveCLI(cfg, &router, analyticsReq)
}

// executeMonitorAction ejecuta una acción individual del monitor
func executeMonitorAction(action, intersection, route string, duration int, router *storage.Router, analyticsReq zmq4.Socket) {
	switch action {
	case "current":
		data, err := router.QueryCurrent(intersection)
		if err != nil {
			log.Printf("[monitor] Error: %v", err)
		} else {
			log.Printf("[monitor] %s", data)
		}
	case "history", "metric_count":
		req := model.MonitorRequest{
			Action:       action,
			Intersection: intersection,
			From:         time.Now().UTC().Add(-2 * time.Minute),
			To:           time.Now().UTC(),
			RequestedAt:  time.Now().UTC(),
		}
		body, _ := json.Marshal(req)
		data, err := router.QueryHistory(body)
		if err != nil {
			log.Printf("[monitor] Error: %v", err)
		} else {
			log.Printf("[monitor] %s", data)
		}
	default:
		req := model.MonitorRequest{
			Action:       action,
			Intersection: intersection,
			Route:        route,
			DurationSec:  duration,
			From:         time.Now().UTC().Add(-2 * time.Minute),
			To:           time.Now().UTC(),
			RequestedAt:  time.Now().UTC(),
		}
		body, _ := json.Marshal(req)
		if err := analyticsReq.Send(zmq4.NewMsg(body)); err != nil {
			log.Fatalf("send analytics request: %v", err)
		}
		msg, err := analyticsReq.Recv()
		if err != nil {
			log.Fatalf("recv analytics response: %v", err)
		}
		if len(msg.Frames) > 0 {
			log.Printf("[monitor] %s", msg.Frames[0])
		}
	}
}

// runMonitorInteractiveCLI ejecuta el CLI interactivo del monitor
func runMonitorInteractiveCLI(cfg config.CityConfig, router *storage.Router, analyticsReq zmq4.Socket) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("[monitor-cli]> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		action := parts[0]

		switch strings.ToLower(action) {
		case "help":
			fmt.Println(`
Comandos disponibles:
  current <intersection>          - Ver estado actual de una intersección
  history <intersection>           - Ver histórico de una intersección
  metric_count <intersection>      - Contar eventos en rango de tiempo
  force_green <intersection>       - Forzar luz verde
  force_green_wave <route>         - Forzar onda verde en ruta
  restore_automatic <intersection> - Restaurar control automático
  health                          - Chequear salud del sistema
  exit, quit                       - Salir
  help                            - Ver este mensaje
			`)

		case "exit", "quit":
			fmt.Println("[monitor-cli] Saliendo...")
			return

		case "current":
			if len(parts) < 2 {
				fmt.Println("Uso: current <intersection>")
				continue
			}
			intersection := parts[1]
			data, err := router.QueryCurrent(intersection)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("[monitor-cli] %s\n", data)
			}

		case "history", "metric_count":
			if len(parts) < 2 {
				fmt.Printf("Uso: %s <intersection>\n", action)
				continue
			}
			intersection := parts[1]
			req := model.MonitorRequest{
				Action:       action,
				Intersection: intersection,
				From:         time.Now().UTC().Add(-2 * time.Minute),
				To:           time.Now().UTC(),
				RequestedAt:  time.Now().UTC(),
			}
			body, _ := json.Marshal(req)
			data, err := router.QueryHistory(body)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("[monitor-cli] %s\n", data)
			}

		default:
			// Enviar comando a analytics (force_green, force_green_wave, restore_automatic, health, etc.)
			lastPart := parts[len(parts)-1]
			req := model.MonitorRequest{
				Action:       action,
				Intersection: lastPart,
				Route:        lastPart,
				DurationSec:  20,
				RequestedAt:  time.Now().UTC(),
			}
			body, _ := json.Marshal(req)
			if err := analyticsReq.Send(zmq4.NewMsg(body)); err != nil {
				fmt.Printf("Error enviando a analytics: %v\n", err)
			} else {
				msg, err := analyticsReq.Recv()
				if err != nil {
					fmt.Printf("Error recibiendo de analytics: %v\n", err)
				} else if len(msg.Frames) > 0 {
					fmt.Printf("[monitor-cli] %s\n", msg.Frames[0])
				}
			}
		}
	}
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

func exitOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
