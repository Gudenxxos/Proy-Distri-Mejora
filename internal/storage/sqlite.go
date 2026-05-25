package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"proy-distri/internal/model"
)

// Store encapsula la conexion SQLite y operaciones de persistencia/consulta.
type Store struct {
	db *sql.DB
}

// storeTimeZone define el huso horario de registro para eventos en BD.
var storeTimeZone = time.FixedZone("UTC-5", -5*60*60)

// NowStoreTime retorna la hora actual en UTC-5
func NowStoreTime() time.Time {
	return time.Now().In(storeTimeZone)
}

func toStoreTime(value time.Time) string {
	return value.In(storeTimeZone).Format(time.RFC3339Nano)
}

func parseStoreTime(value string) (time.Time, error) {
	return time.ParseInLocation(time.RFC3339Nano, value, storeTimeZone)
}

// Open inicializa una tienda SQLite y asegura el esquema requerido.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	store := &Store{db: db}
	if err := store.initSchema(); err != nil {
		return nil, err
	}

	return store, nil
}

// Close cierra la conexion activa a la base de datos.
func (s *Store) Close() error {
	return s.db.Close()
}

// initSchema crea las tablas base usadas por analytics y monitor.
func (s *Store) initSchema() error {
	schema := `
CREATE TABLE IF NOT EXISTS traffic_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id TEXT,
  kind TEXT NOT NULL,
  topic TEXT NOT NULL,
  intersection TEXT,
  has_semaphore INTEGER DEFAULT 0,
  status TEXT,
  light_state TEXT,
  queue_length INTEGER,
  avg_speed REAL,
  density REAL,
  raw_payload TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS light_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id TEXT NOT NULL,
  intersection TEXT NOT NULL,
  target_state TEXT NOT NULL,
  duration_sec INTEGER NOT NULL,
  reason TEXT NOT NULL,
  requested_by TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  changed_at TEXT
);`

	if _, err := s.db.Exec(schema); err != nil {
		return err
	}

	_, _ = s.db.Exec(`ALTER TABLE traffic_events ADD COLUMN event_id TEXT`)
	_, _ = s.db.Exec(`ALTER TABLE traffic_events ADD COLUMN has_semaphore INTEGER DEFAULT 0`)
	return nil
}

// InsertEnvelope persiste snapshots, comandos y metadatos en la BD.
func (s *Store) InsertEnvelope(env model.PersistEnvelope) error {
	raw := env.RawPayload
	intersection := ""
	status := ""
	lightState := ""
	queue := 0
	avgSpeed := 0.0
	density := 0.0
	hasSemaphore := 0

	if env.Snapshot != nil {
		intersection = env.Snapshot.Intersection
		if env.Snapshot.HasSemaphore {
			hasSemaphore = 1
		}
		status = env.Snapshot.Status
		lightState = env.Snapshot.LightState
		queue = env.Snapshot.QueueLength
		avgSpeed = env.Snapshot.AvgSpeed
		density = env.Snapshot.Density
	}

	if env.LightState != nil {
		intersection = env.LightState.Intersection
		lightState = env.LightState.LightState
	}

	if raw == "" {
		bytes, _ := json.Marshal(env)
		raw = string(bytes)
	}

	_, err := s.db.Exec(
		`INSERT INTO traffic_events
		(event_id, kind, topic, intersection, has_semaphore, status, light_state, queue_length, avg_speed, density, raw_payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		env.EventID,
		env.Kind,
		env.Topic,
		intersection,
		hasSemaphore,
		status,
		lightState,
		queue,
		avgSpeed,
		density,
		raw,
		toStoreTime(env.CreatedAt),
	)
	if err != nil {
		return err
	}

	if env.LightCommand != nil {
		var changedAtStr *string
		if env.LightCommand.ChangedAt != nil {
			t := toStoreTime(*env.LightCommand.ChangedAt)
			changedAtStr = &t
		}
		_, err = s.db.Exec(
			`INSERT INTO light_actions
			(command_id, intersection, target_state, duration_sec, reason, requested_by, requested_at, changed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			env.LightCommand.CommandID,
			env.LightCommand.Intersection,
			env.LightCommand.TargetState,
			env.LightCommand.DurationSec,
			env.LightCommand.Reason,
			env.LightCommand.RequestedBy,
			toStoreTime(env.LightCommand.RequestedAt),
			changedAtStr,
		)
	}

	if err == nil && env.LightState != nil && env.LightState.CommandID != "" {
		err = s.MarkLightChanged(env.LightState.CommandID, env.LightState.ChangedAt)
	}

	return err
}

// MarkLightChanged marca la hora efectiva de ejecucion de un comando.
func (s *Store) MarkLightChanged(commandID string, changedAt time.Time) error {
	_, err := s.db.Exec(
		`UPDATE light_actions SET changed_at = ? WHERE command_id = ?`,
		toStoreTime(changedAt),
		commandID,
	)
	return err
}

// QueryCurrent devuelve el ultimo snapshot conocido de una interseccion.
func (s *Store) QueryCurrent(intersection string) ([]model.IntersectionSnapshot, error) {
	rows, err := s.db.Query(
		`SELECT intersection, has_semaphore, queue_length, avg_speed, density, light_state, status, created_at
		FROM traffic_events
		WHERE intersection = ?
		ORDER BY id DESC
		LIMIT 1`,
		intersection,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.IntersectionSnapshot
	for rows.Next() {
		var snapshot model.IntersectionSnapshot
		var createdAt string
		var hasSemaphore int
		if err := rows.Scan(
			&snapshot.Intersection,
			&hasSemaphore,
			&snapshot.QueueLength,
			&snapshot.AvgSpeed,
			&snapshot.Density,
			&snapshot.LightState,
			&snapshot.Status,
			&createdAt,
		); err != nil {
			return nil, err
		}
		snapshot.HasSemaphore = hasSemaphore == 1
		snapshot.UpdatedAt, _ = parseStoreTime(createdAt)
		out = append(out, snapshot)
	}
	return out, rows.Err()
}

// QueryHistory devuelve eventos en un rango temporal.
func (s *Store) QueryHistory(from, to time.Time) ([]map[string]any, error) {
	rows, err := s.db.Query(
		`SELECT kind, topic, intersection, status, light_state, queue_length, avg_speed, density, created_at
		FROM traffic_events
		WHERE created_at BETWEEN ? AND ?
		ORDER BY id ASC`,
		toStoreTime(from),
		toStoreTime(to),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]map[string]any, 0)
	for rows.Next() {
		var (
			kind, topic, intersection, status, lightState, createdAt string
			queue                                                    int
			avgSpeed, density                                        float64
		)
		if err := rows.Scan(&kind, &topic, &intersection, &status, &lightState, &queue, &avgSpeed, &density, &createdAt); err != nil {
			return nil, err
		}
		results = append(results, map[string]any{
			"kind":         kind,
			"topic":        topic,
			"intersection": intersection,
			"status":       status,
			"light_state":  lightState,
			"queue_length": queue,
			"avg_speed":    avgSpeed,
			"density":      density,
			"created_at":   createdAt,
		})
	}
	return results, rows.Err()
}

// CountBetween cuenta eventos persistidos dentro de un rango temporal.
func (s *Store) CountBetween(from, to time.Time) (int, error) {
	row := s.db.QueryRow(
		`SELECT COUNT(*) FROM traffic_events WHERE created_at BETWEEN ? AND ?`,
		toStoreTime(from),
		toStoreTime(to),
	)

	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

// String devuelve una representacion corta de la instancia de Store.
func (s *Store) String() string {
	return fmt.Sprintf("store<%p>", s)
}
