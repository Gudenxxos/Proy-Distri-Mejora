package main

import (
	"context"
	"log"
	"os"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
)

// main inicia el broker y arma el pipeline concurrente de recepcion y fanout.
func main() {
	cfgPath := getenv("CITY_CONFIG", "configs/city.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx := context.Background()
	sub := zmq4.NewSub(ctx)
	defer sub.Close()

	if err := sub.Listen(cfg.Endpoints.BrokerIngest); err != nil {
		log.Fatalf("listen ingest: %v", err)
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, ""); err != nil {
		log.Fatalf("subscribe all: %v", err)
	}

	pub := zmq4.NewPub(ctx)
	defer pub.Close()
	if err := pub.Listen(cfg.Endpoints.BrokerFanout); err != nil {
		log.Fatalf("listen fanout: %v", err)
	}

	workers := map[string]chan zmq4.Msg{
		model.TopicCamera:    make(chan zmq4.Msg, 64),
		model.TopicGPS:       make(chan zmq4.Msg, 64),
		model.TopicInductive: make(chan zmq4.Msg, 64),
	}
	fanout := make(chan zmq4.Msg, 128)

	for topic, queue := range workers {
		go func(topic string, queue <-chan zmq4.Msg) {
			log.Printf("[broker-worker] activo para topic=%s", topic)
			for msg := range queue {
				fanout <- msg
			}
		}(topic, queue)
	}

	go func() {
		for msg := range fanout {
			if len(msg.Frames) == 0 {
				continue
			}
			if err := pub.Send(msg); err != nil {
				log.Printf("[broker] send error: %v", err)
			}
		}
	}()

	log.Printf("[broker] pipeline concurrente listo en %s -> %s", cfg.Endpoints.BrokerIngest, cfg.Endpoints.BrokerFanout)

	for {
		msg, err := sub.Recv()
		if err != nil {
			log.Printf("[broker] recv error: %v", err)
			continue
		}
		if len(msg.Frames) == 0 {
			continue
		}

		topic := string(msg.Frames[0])
		queue, ok := workers[topic]
		if !ok {
			log.Printf("[broker] topic no soportado=%s", topic)
			continue
		}

		log.Printf("[broker] topic=%s", topic)
		queue <- cloneMsg(msg)
	}
}

// cloneMsg realiza una copia profunda de frames para evitar aliasing entre goroutines.
func cloneMsg(msg zmq4.Msg) zmq4.Msg {
	cloned := make([][]byte, len(msg.Frames))
	for i, frame := range msg.Frames {
		if frame == nil {
			continue
		}
		cloned[i] = append([]byte(nil), frame...)
	}

	return zmq4.Msg{Frames: cloned}
}

// getenv lee una variable de entorno con valor por defecto.
func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
