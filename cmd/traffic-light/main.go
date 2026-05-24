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
		log.Fatalf("listen analytics light queue: %v", err)
	}

	pub := zmq4.NewPub(ctx)
	defer pub.Close()
	if err := pub.Dial(cfg.Endpoints.BrokerIngest); err != nil {
		log.Fatalf("dial broker ingest: %v", err)
	}

	// PUSH para enviar comandos ejecutados de vuelta a analytics
	pushExecuted := zmq4.NewPush(ctx)
	defer pushExecuted.Close()
	if err := pushExecuted.Dial(cfg.Endpoints.TrafficLightExecutedPush); err != nil {
		log.Fatalf("dial traffic light executed push: %v", err)
	}

	states := make(map[string]string)
	timers := make(map[string]chan struct{})
	var mu sync.Mutex

	log.Printf("[traffic-light] esperando comandos en %s", cfg.Endpoints.TrafficLightPull)

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

		mu.Lock()
		states[cmd.Intersection] = cmd.TargetState
		if stop, ok := timers[cmd.Intersection]; ok {
			close(stop)
		}
		stopCh := make(chan struct{})
		timers[cmd.Intersection] = stopCh
		mu.Unlock()

		// Asignar ChangedAt y enviar comando ejecutado a analytics
		now := storage.NowStoreTime()
		cmd.ChangedAt = &now
		
		// Publicar evento en broker
		event := model.LightStateEvent{
			CommandID:    cmd.CommandID,
			Intersection: cmd.Intersection,
			LightState:   cmd.TargetState,
			Reason:       cmd.Reason,
			ChangedAt:    now,
		}
		eventData, _ := json.Marshal(event)
		_ = pub.Send(zmq4.NewMsgFrom([]byte(model.TopicLightState), eventData))

		// Enviar comando ejecutado a analytics para persistencia
		cmdData, _ := json.Marshal(cmd)
		_ = pushExecuted.Send(zmq4.NewMsg(cmdData))
		
		log.Printf("[traffic-light] %s -> %s por %s durante %ds", cmd.Intersection, cmd.TargetState, cmd.Reason, cmd.DurationSec)

		go func(command model.LightCommand, stopCh chan struct{}, pushEx zmq4.Socket) {
			timer := time.NewTimer(time.Duration(command.DurationSec) * time.Second)
			defer timer.Stop()

			select {
			case <-stopCh:
				return
			case <-timer.C:
			}

			nextPhase := model.OppositePhase(command.TargetState)
			mu.Lock()
			states[command.Intersection] = nextPhase
			mu.Unlock()

			now := storage.NowStoreTime()
			event := model.LightStateEvent{
				CommandID:    command.CommandID,
				Intersection: command.Intersection,
				LightState:   nextPhase,
				Reason:       "cycle_end",
				ChangedAt:    now,
			}
			
			// Solo publicar, no persistir aquí
			eventData, _ := json.Marshal(event)
			_ = pub.Send(zmq4.NewMsgFrom([]byte(model.TopicLightState), eventData))
			
			log.Printf("[traffic-light] %s cambia a %s al terminar el ciclo", command.Intersection, nextPhase)
		}(cmd, stopCh, pushExecuted)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
