# Proyecto: Proy-Distri-Mejora

Resumen corto

- Sistema distribuido de gestión y visualización de semáforos e intersecciones urbanas.
- Tecnologías: Go (módulos), ZeroMQ (github.com/go-zeromq/zmq4), SQLite (modernc.org/sqlite).
- Arquitectura basada en productores (sensores), un `broker` de fanout, servicios de `analytics`, `traffic-light`, `visualizer`, `db-server` y utilidades (`monitor`, `sensor-node`).

Estado del repositorio

- Lenguaje: Go (recomendado Go 1.20+; el repo se probó con Go 1.26.1 en este entorno).
- Compilación: proyecto modular con `go.mod` en la raíz.

Estructura principal

- `cmd/` — Binaries ejecutables:
  - `cmd/broker` — Broker PUB/SUB que recibe de sensores y reenvía al resto.
  - `cmd/analytics` — Evaluador de reglas, persistencia y emisión de órdenes a `traffic-light`.
  - `cmd/traffic-light` — Ejecutor de comandos de semáforo y emisor de eventos ejecutados.
  - `cmd/visualizer` — Consumidor y servidor web para visualización (SSE/HTTP).
  - `cmd/db-server` — API de persistencia sobre SQLite (consulta/replica, monitor).
  - `cmd/monitor` — Cliente CLI para consultas ad-hoc y debugging.
  - `cmd/sensor-node` — Simulador/generador de eventos de sensores.
- `internal/` — Lógica interna y modelos:
  - `internal/analytics` — Reglas y evaluación de congestión/priority.
  - `internal/storage` — Adaptador SQLite y esquema (tabla `traffic_events`).
  - `internal/model` — Tipos de dominio y eventos compartidos.
  - `internal/config` — Carga de `configs/city.json` y perfiles de sensores.
  - `internal/web` — Assets embebidos para `visualizer`.
- `configs/city.json` — Configuración de ciudad, intersecciones y sensores.

Puntos de diseño importantes

- Broker concurrente: `cmd/broker` usa canales por tópico y un único goroutine PUB para serializar envíos a ZeroMQ (evita compartir sockets entre goroutines).
- Event correlation: `analytics` genera `event_id` (UUID) para cada envoltura (`traffic_events`) y se persiste en ambos DBs para trazabilidad.
- Protección de semáforos forzados: `traffic-light` aplica locks por intersección durante `force_green`/`force_green_wave` para evitar cambios concurrentes.
- Evaluación de congestión: `analytics` y `visualizer` detectan si una intersección tiene sensor de velocidad; si no lo tiene, se evita usar `AvgSpeed==0` como indicador de congestión.

Base de datos y esquema

- Motor: SQLite (embedded via `modernc.org/sqlite`).
- Tabla principal: `traffic_events` (ahora incluye `event_id TEXT`, `has_semaphore`, `status`, `light_state`, `queue_length`, `avg_speed`, `density`, `raw_payload`, `created_at`).
- Nota: Los scripts `start-pc2.bat` y `start-pc3.sh` borran las bases de datos (`replica.db`/`primary.db`) antes de compilar para entornos de desarrollo limpias.

Requisitos y compilación

- Instala Go: https://go.dev/ (recomendado 1.20+).
- Desde la raíz del repo:

```bash
go test ./...    # Ejecuta tests unitarios (internal/*)
go build ./cmd/...   # Compila todos los binarios si es necesario
```

Ejecución (entorno de desarrollo)

- Para correr el sistema distribuido en máquinas separadas, use los scripts provistos:
  - [start-pc2.bat](start-pc2.bat) — para Windows (PC2).
  - [start-pc3.sh](start-pc3.sh) — para Unix (PC3).
- Alternativamente, puede iniciar binarios manualmente en el orden apropiado:
  1. `cmd/broker` (fanout)
  2. `cmd/db-server` (persistencia)
  3. `cmd/analytics` (procesador de reglas)
  4. `cmd/traffic-light` (actuador)
  5. `cmd/visualizer` (UI)
  6. `cmd/sensor-node` (simuladores de sensores)

Notas operativas y debugging

- Monitor: `cmd/monitor` ofrece consultas `current` y formatea la salida para facilitar lectura humana; también tolera respuestas envueltas por `MonitorResponse`.
- Logs y trazabilidad: cada evento persistente incluye `event_id` para correlacionar en primary/replica.
- ZeroMQ: no comparta sockets entre goroutines; use patrón "single writer" o sockets dedicados por goroutine.

Dónde mirar primero

- Para entender la canalización de mensajes: [cmd/broker/main.go](cmd/broker/main.go)
- Para las reglas de congestión y prioridad: [internal/analytics/rules.go](internal/analytics/rules.go)
- Para persistencia y esquema: [internal/storage/sqlite.go](internal/storage/sqlite.go)
- Para la UI y heurísticas de visualización: [cmd/visualizer/main.go](cmd/visualizer/main.go)
