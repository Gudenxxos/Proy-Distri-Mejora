package main

import (
	"context"
	"encoding/json"
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

	role := strings.ToLower(getenv("DB_ROLE", "primary"))
	dbPath := getenv("DB_PATH", role+".db")
	store, err := storage.Open(dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	pull := zmq4.NewPull(ctx)
	rep := zmq4.NewRep(ctx)
	defer pull.Close()
	defer rep.Close()

	pullEndpoint := cfg.Endpoints.DBPrimaryPull
	repEndpoint := cfg.Endpoints.DBPrimaryREP
	if role == "replica" {
		pullEndpoint = cfg.Endpoints.DBReplicaPull
		repEndpoint = cfg.Endpoints.DBReplicaREP
	}

	if err := pull.Listen(pullEndpoint); err != nil {
		log.Fatalf("listen pull endpoint: %v", err)
	}
	if err := rep.Listen(repEndpoint); err != nil {
		log.Fatalf("listen rep endpoint: %v", err)
	}

	log.Printf("[db-server:%s] sqlite=%s pull=%s rep=%s", role, dbPath, pullEndpoint, repEndpoint)

	go func() {
		for {
			msg, err := pull.Recv()
			if err != nil {
				log.Printf("db pull recv: %v", err)
				continue
			}
			if len(msg.Frames) == 0 {
				continue
			}
			var env model.PersistEnvelope
			if err := json.Unmarshal(msg.Frames[0], &env); err != nil {
				log.Printf("decode envelope: %v", err)
				continue
			}
			if err := store.InsertEnvelope(env); err != nil {
				log.Printf("insert envelope: %v", err)
				continue
			}
			log.Printf("[db-server:%s] persisted %s %s", role, env.Kind, env.Topic)
		}
	}()

	for {
		msg, err := rep.Recv()
		if err != nil {
			log.Fatalf("rep recv: %v", err)
		}
		if len(msg.Frames) == 0 {
			_ = rep.Send(zmq4.NewMsgString(`{"success":false,"message":"empty request"}`))
			continue
		}

		var req model.MonitorRequest
		if err := json.Unmarshal(msg.Frames[0], &req); err != nil {
			_ = rep.Send(zmq4.NewMsgString(`{"success":false,"message":"invalid json"}`))
			continue
		}

		resp := handleQuery(store, req)
		data, _ := json.Marshal(resp)
		_ = rep.Send(zmq4.NewMsg(data))
	}
}

func handleQuery(store *storage.Store, req model.MonitorRequest) model.MonitorResponse {
	switch strings.ToLower(req.Action) {
	case "health":
		// Health check endpoint para detectar disponibilidad del DB server
		return model.MonitorResponse{Success: true, Message: "db_primary_healthy", Data: map[string]any{"timestamp": time.Now().UTC()}}
	case "current":
		data, err := store.QueryCurrent(req.Intersection)
		if err != nil {
			return model.MonitorResponse{Success: false, Message: err.Error()}
		}
		return model.MonitorResponse{Success: true, Message: "consulta puntual", Data: data}
	case "history":
		if req.From.IsZero() {
			req.From = time.Now().UTC().Add(-2 * time.Minute)
		}
		if req.To.IsZero() {
			req.To = time.Now().UTC()
		}
		data, err := store.QueryHistory(req.From, req.To)
		if err != nil {
			return model.MonitorResponse{Success: false, Message: err.Error()}
		}
		return model.MonitorResponse{Success: true, Message: "consulta historica", Data: data}
	case "metric_count":
		if req.From.IsZero() {
			req.From = time.Now().UTC().Add(-2 * time.Minute)
		}
		if req.To.IsZero() {
			req.To = time.Now().UTC()
		}
		count, err := store.CountBetween(req.From, req.To)
		if err != nil {
			return model.MonitorResponse{Success: false, Message: err.Error()}
		}
		return model.MonitorResponse{Success: true, Message: "conteo de eventos", Data: map[string]any{"count": count}}
	default:
		return model.MonitorResponse{Success: false, Message: "accion no soportada por la BD"}
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
