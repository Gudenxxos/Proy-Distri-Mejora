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

	pushPrimary := zmq4.NewPush(ctx)
	defer pushPrimary.Close()
	if err := pushPrimary.Dial(cfg.Endpoints.DBPrimaryPull); err != nil {
		log.Fatalf("dial primary db push: %v", err)
	}

	pushReplica := zmq4.NewPush(ctx)
	defer pushReplica.Close()
	if err := pushReplica.Dial(cfg.Endpoints.DBReplicaPull); err != nil {
		log.Fatalf("dial replica db push: %v", err)
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

		event := model.LightStateEvent{
			CommandID:    cmd.CommandID,
			Intersection: cmd.Intersection,
			LightState:   cmd.TargetState,
			Reason:       cmd.Reason,
			ChangedAt:    time.Now().UTC(),
		}

		persistAndPublish(event, pub, pushPrimary, pushReplica)
		log.Printf("[traffic-light] %s -> %s por %s durante %ds", cmd.Intersection, cmd.TargetState, cmd.Reason, cmd.DurationSec)

		go func(command model.LightCommand, stopCh chan struct{}) {
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

			event := model.LightStateEvent{
				CommandID:    command.CommandID,
				Intersection: command.Intersection,
				LightState:   nextPhase,
				Reason:       "cycle_end",
				ChangedAt:    time.Now().UTC(),
			}
			persistAndPublish(event, pub, pushPrimary, pushReplica)
			log.Printf("[traffic-light] %s cambia a %s al terminar el ciclo", command.Intersection, nextPhase)
		}(cmd, stopCh)
	}
}

func persistAndPublish(event model.LightStateEvent, pub, pushPrimary, pushReplica zmq4.Socket) {
	data, _ := json.Marshal(event)
	_ = pub.Send(zmq4.NewMsgFrom([]byte(model.TopicLightState), data))

	env := model.PersistEnvelope{
		Kind:       "light_state",
		Topic:      model.TopicLightState,
		RawPayload: string(data),
		LightState: &event,
		CreatedAt:  event.ChangedAt,
	}
	bytes, _ := json.Marshal(env)
	_ = pushPrimary.Send(zmq4.NewMsg(bytes))
	_ = pushReplica.Send(zmq4.NewMsg(bytes))
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
