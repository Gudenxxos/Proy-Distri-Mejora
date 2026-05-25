package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"

	"proy-distri/internal/config"
	"proy-distri/internal/model"
)

// main levanta publicadores concurrentes para cada perfil de sensor configurado.
func main() {
	rand.Seed(time.Now().UnixNano())

	cfgPath := getenv("CITY_CONFIG", "configs/city.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	pub := zmq4.NewPub(context.Background())
	defer pub.Close()

	if err := pub.Dial(cfg.Endpoints.BrokerIngest); err != nil {
		log.Fatalf("dial broker ingest: %v", err)
	}

	log.Printf("[sensor-node] publicando hacia %s", cfg.Endpoints.BrokerIngest)

	for _, profile := range cfg.SensorProfiles {
		profile := profile
		go func() {
			ticker := time.NewTicker(time.Duration(profile.IntervalSeconds) * time.Second)
			defer ticker.Stop()

			for {
				topic, event := buildEvent(profile)
				publish(pub, topic, event)
				<-ticker.C
			}
		}()
	}

	select {}
}

// publish serializa y envia un evento al broker usando topico + payload.
func publish(pub zmq4.Socket, topic string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("marshal payload: %v", err)
		return
	}

	if err := pub.Send(zmq4.NewMsgFrom([]byte(topic), data)); err != nil {
		log.Printf("publish %s: %v", topic, err)
		return
	}

	log.Printf("[sensor-node] topic=%s payload=%s", topic, string(data))
}

// buildEvent construye eventos sinteticos segun el tipo de sensor.
func buildEvent(profile config.SensorProfile) (string, any) {
	now := time.Now().UTC()
	baseQueue := rand.Intn(9) + 1
	baseSpeed := float64(rand.Intn(15) + 10)
	baseDensity := float64(rand.Intn(10) + 30)

	switch strings.ToLower(profile.SensorType) {
	case "camara":
		return model.TopicCamera, model.CameraEvent{
			SensorID:          profile.SensorID,
			TipoSensor:        "camara",
			Interseccion:      profile.Intersection,
			Volumen:           baseQueue,
			VelocidadPromedio: baseSpeed,
			Timestamp:         now,
		}
	case "espira_inductiva":
		return model.TopicInductive, model.InductiveEvent{
			SensorID:          profile.SensorID,
			TipoSensor:        "espira_inductiva",
			Interseccion:      profile.Intersection,
			VehiculosContados: rand.Intn(8) + 1,
			IntervaloSegundos: profile.IntervalSeconds,
			TimestampInicio:   now.Add(-time.Duration(profile.IntervalSeconds) * time.Second),
			TimestampFin:      now,
		}
	default:
		level := "NORMAL"
		if baseSpeed < 20 {
			level = "ALTA"
		} else if baseSpeed > 40 {
			level = "BAJA"
		}
		return model.TopicGPS, model.GPSEvent{
			SensorID:          profile.SensorID,
			TipoSensor:        "gps",
			Interseccion:      profile.Intersection,
			NivelCongestion:   level,
			Densidad:          baseDensity,
			VelocidadPromedio: baseSpeed,
			Timestamp:         now,
		}
	}
}

// getenv lee una variable de entorno con fallback.
func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
