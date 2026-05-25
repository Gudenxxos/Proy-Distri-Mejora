package main

import (
	"context"
	"log"
	"os"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
)

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

	log.Printf("[broker] escuchando eventos en %s y reenviando en %s", cfg.Endpoints.BrokerIngest, cfg.Endpoints.BrokerFanout)

	for {
		msg, err := sub.Recv()
		if err != nil {
			log.Printf("[broker] recv error: %v", err)
			continue
		}
		if len(msg.Frames) == 0 {
			continue
		}
		log.Printf("[broker] topic=%s", string(msg.Frames[0]))
		if err := pub.Send(msg); err != nil {
			log.Fatalf("send: %v", err)
		}
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
