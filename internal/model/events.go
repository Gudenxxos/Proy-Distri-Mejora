package model

import "time"

// Topicos y constantes de dominio compartidas entre procesos.
const (
	TopicCamera     = "sensor.camera"
	TopicGPS        = "sensor.gps"
	TopicInductive  = "sensor.inductive"
	TopicSnapshot   = "traffic.snapshot"
	TopicCommand    = "traffic.command"
	TopicLightState = "traffic.lightstate"
	ActionForceGreen = "force_green"

	LightPhaseVertical   = "VERTICAL_GREEN"
	LightPhaseHorizontal = "HORIZONTAL_GREEN"
	LightPhaseNone       = "NONE"

	FlowAxisVertical   = "VERTICAL"
	FlowAxisHorizontal = "HORIZONTAL"
)

// CameraEvent representa una lectura puntual de camara.
type CameraEvent struct {
	SensorID          string    `json:"sensor_id"`
	TipoSensor        string    `json:"tipo_sensor"`
	Interseccion      string    `json:"interseccion"`
	Volumen           int       `json:"volumen"`
	VelocidadPromedio float64   `json:"velocidad_promedio"`
	Timestamp         time.Time `json:"timestamp"`
}

// InductiveEvent representa conteos acumulados de espiras.
type InductiveEvent struct {
	SensorID          string    `json:"sensor_id"`
	TipoSensor        string    `json:"tipo_sensor"`
	Interseccion      string    `json:"interseccion"`
	VehiculosContados int       `json:"vehiculos_contados"`
	IntervaloSegundos int       `json:"intervalo_segundos"`
	TimestampInicio   time.Time `json:"timestamp_inicio"`
	TimestampFin      time.Time `json:"timestamp_fin"`
}

// GPSEvent representa una lectura de congestion sintetica por GPS.
type GPSEvent struct {
	SensorID          string    `json:"sensor_id"`
	TipoSensor        string    `json:"tipo_sensor"`
	Interseccion      string    `json:"interseccion"`
	NivelCongestion   string    `json:"nivel_congestion"`
	Densidad          float64   `json:"densidad"`
	VelocidadPromedio float64   `json:"velocidad_promedio"`
	Timestamp         time.Time `json:"timestamp"`
}

// IntersectionSnapshot resume el estado actual de una interseccion.
type IntersectionSnapshot struct {
	Intersection    string    `json:"intersection"`
	QueueLength     int       `json:"queue_length"`
	AvgSpeed        float64   `json:"avg_speed"`
	Density         float64   `json:"density"`
	VehiclesCounted int       `json:"vehicles_counted"`
	LightState      string    `json:"light_state"`
	HasSemaphore    bool      `json:"has_semaphore"`
	Status          string    `json:"status"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// LightCommand representa una orden de cambio de fase de semaforo.
type LightCommand struct {
	CommandID     string     `json:"command_id"`
	Intersection  string     `json:"intersection"`
	TargetState   string     `json:"target_state"`
	DurationSec   int        `json:"duration_sec"`
	Reason        string     `json:"reason"`
	RequestedBy   string     `json:"requested_by"`
	RequestedAt   time.Time  `json:"requested_at"`
	ChangedAt     *time.Time `json:"changed_at,omitempty"` // Asignado por traffic-light cuando ejecuta el comando
	PriorityRoute string     `json:"priority_route,omitempty"`
}

// LightStateEvent notifica un cambio aplicado de estado de semaforo.
type LightStateEvent struct {
	CommandID    string    `json:"command_id,omitempty"`
	Intersection string    `json:"intersection"`
	LightState   string    `json:"light_state"`
	Reason       string    `json:"reason"`
	ChangedAt    time.Time `json:"changed_at"`
}

// MonitorRequest modela una solicitud de operacion del monitor.
type MonitorRequest struct {
	Action       string    `json:"action"`
	Intersection string    `json:"intersection,omitempty"`
	Route        string    `json:"route,omitempty"`
	From         time.Time `json:"from,omitempty"`
	To           time.Time `json:"to,omitempty"`
	DurationSec  int       `json:"duration_sec,omitempty"`
	RequestedAt  time.Time `json:"requested_at"`
}

// MonitorResponse encapsula una respuesta estandar de monitor/servicios.
type MonitorResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// PersistEnvelope define el formato comun para persistencia en BD.
type PersistEnvelope struct {
	Kind         string                `json:"kind"`
	EventID      string                `json:"event_id"`
	Topic        string                `json:"topic"`
	RawPayload   string                `json:"raw_payload"`
	Snapshot     *IntersectionSnapshot `json:"snapshot,omitempty"`
	LightCommand *LightCommand         `json:"light_command,omitempty"`
	LightState   *LightStateEvent      `json:"light_state,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
}
