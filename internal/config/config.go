package config

import (
	"encoding/json"
	"os"
)

type Endpoints struct {
	BrokerIngest              string `json:"broker_ingest"`
	BrokerFanout              string `json:"broker_fanout"`
	AnalyticsREP              string `json:"analytics_rep"`
	TrafficLightPull          string `json:"traffic_light_pull"`
	TrafficLightExecutedPush  string `json:"traffic_light_executed_push"` // Donde traffic-light envía comandos ejecutados
	DBPrimaryPull             string `json:"db_primary_pull"`
	DBReplicaPull             string `json:"db_replica_pull"`
	DBPrimaryREP              string `json:"db_primary_rep"`
	DBReplicaREP              string `json:"db_replica_rep"`
	VisualizerHTTP            string `json:"visualizer_http"`
}

type IntersectionConfig struct {
	ID           string `json:"id"`
	Row          string `json:"row"`
	Col          int    `json:"col"`
	HasSemaphore bool   `json:"has_semaphore"`
	Upstream     string `json:"upstream"`
	Monitored    bool   `json:"monitored"`
}

type SensorProfile struct {
	SensorID        string `json:"sensor_id"`
	SensorType      string `json:"sensor_type"`
	Intersection    string `json:"intersection"`
	IntervalSeconds int    `json:"interval_seconds"`
}

type CityConfig struct {
	CityName                   string               `json:"city_name"`
	MatrixRows                 int                  `json:"matrix_rows"`
	MatrixCols                 int                  `json:"matrix_cols"`
	BaseGreenSeconds           int                  `json:"base_green_seconds"`
	CongestionExtensionSeconds int                  `json:"congestion_extension_seconds"`
	PriorityWaveSeconds        int                  `json:"priority_wave_seconds"`
	HealthCheckIntervalMS      int                  `json:"health_check_interval_ms"`
	HealthCheckTimeoutMS       int                  `json:"health_check_timeout_ms"`
	Endpoints                  Endpoints            `json:"endpoints"`
	Intersections              []IntersectionConfig `json:"intersections"`
	SensorProfiles             []SensorProfile      `json:"sensor_profiles"`
}

func Load(path string) (CityConfig, error) {
	var cfg CityConfig

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
