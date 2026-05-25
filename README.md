# Proy-Distri-Mejora

Sistema distribuido para simulacion, gestion y visualizacion de trafico urbano en una malla de intersecciones con semaforos, sensores simulados, analitica de congestion, actuacion automatica y persistencia replicada en SQLite.

## Descripcion

`Proy-Distri-Mejora` modela una ciudad 4x4 donde distintos procesos cooperan mediante ZeroMQ para recibir eventos de sensores, detectar congestion, modificar fases de semaforos, consultar historicos y visualizar el estado de las intersecciones en tiempo real.

El proyecto tiene un enfoque academico/profesional orientado a sistemas distribuidos: separa productores, broker, procesamiento, actuadores, almacenamiento primario/replica, monitor CLI y visualizador web.

## Caracteristicas

- Simulacion de sensores de camara, GPS y espira inductiva.
- Broker concurrente PUB/SUB para fanout de eventos por topico.
- Motor de reglas para deteccion de congestion por cola, densidad y velocidad promedio.
- Control de semaforos con fases `HORIZONTAL_GREEN` y `VERTICAL_GREEN`.
- Ciclo automatico de semaforos con temporizadores por interseccion.
- Comandos manuales desde monitor: `force_green`, `force_green_wave` y `restore_automatic`.
- Bloqueo temporal para comandos de prioridad, evitando cambios concurrentes durante una intervencion manual.
- Persistencia en SQLite con servidor primario y replica.
- Failover de consultas desde monitor hacia replica cuando el primario no responde.
- Health check periodico del servidor de base de datos primario.
- Visualizador HTTP con Server-Sent Events para actualizaciones en tiempo real.
- Scripts de arranque por maquina/rol para una practica distribuida en varias PCs.

## Tecnologias utilizadas

- **Go**: lenguaje principal del proyecto.
- **Go modules**: gestion de dependencias mediante `go.mod` y `go.sum`.
- **ZeroMQ**: comunicacion entre servicios usando `github.com/go-zeromq/zmq4`.
- **SQLite**: persistencia embebida mediante `modernc.org/sqlite`.
- **HTTP + SSE**: visualizador web en tiempo real.
- **Shell/Bash y Batch**: scripts de compilacion y arranque para Linux/Unix y Windows.

Dependencias principales detectadas:

```text
github.com/go-zeromq/zmq4
modernc.org/sqlite
github.com/google/uuid
```

## Arquitectura

El sistema sigue una arquitectura distribuida orientada a eventos. Cada servicio vive como un binario independiente dentro de `cmd/` y comparte tipos de dominio desde `internal/`.

```text
sensor-node
    |
    | PUB sensor.camera / sensor.gps / sensor.inductive
    v
broker
    |
    | fanout PUB
    v
analytics -----------------------> traffic-light
    |                                  |
    | persist envelopes                | comandos ejecutados / cambios de fase
    v                                  v
db-server primary / replica       analytics + visualizer

monitor <------ REQ/REP ------ analytics / db-server
visualizer <--- SSE/HTTP ----- navegador
```

### Servicios principales

| Servicio      | Ruta                | Responsabilidad                                                                        |
| ------------- | ------------------- | -------------------------------------------------------------------------------------- |
| Broker        | `cmd/broker`        | Recibe eventos de sensores y los redistribuye por topico.                              |
| Sensor Node   | `cmd/sensor-node`   | Genera eventos sinteticos segun `sensor_profiles`.                                     |
| Analytics     | `cmd/analytics`     | Procesa eventos, actualiza modelo de ciudad, evalua reglas y emite comandos.           |
| Traffic Light | `cmd/traffic-light` | Ejecuta cambios de semaforo, alterna ciclos automaticos y publica comandos ejecutados. |
| DB Server     | `cmd/db-server`     | Persiste eventos y responde consultas en rol `primary` o `replica`.                    |
| Monitor       | `cmd/monitor`       | CLI interactivo y cliente de consultas/comandos.                                       |
| Visualizer    | `cmd/visualizer`    | Servidor web del tablero en tiempo real.                                               |

### Patrones utilizados

- **Event-driven architecture** con eventos serializados en JSON.
- **Pub/Sub y Push/Pull** mediante ZeroMQ.
- **REQ/REP** para consultas y comandos sincronos.
- **Separacion por capas internas**:
  - `internal/model`: eventos y estado de dominio.
  - `internal/config`: carga de configuracion.
  - `internal/analytics`: reglas de negocio.
  - `internal/storage`: persistencia SQLite y router de failover.
  - `internal/web`: HTML embebido para visualizacion.
- **Primary/Replica** para persistencia y consultas.
- **Circuit breaker parcial** en `analytics`: si el DB primario falla, redirige persistencia a replica.

## Estructura del proyecto

```text
.
+-- cmd/
|   +-- analytics/       # Motor de reglas, health checks y comandos a semaforos
|   +-- broker/          # Broker ZeroMQ de entrada y fanout
|   +-- db-server/       # Servidor SQLite en modo primary o replica
|   +-- monitor/         # Cliente CLI interactivo
|   +-- sensor-node/     # Simulador de sensores
|   +-- traffic-light/   # Actuador de semaforos
|   +-- visualizer/      # Servidor HTTP/SSE
+-- configs/
|   +-- city.json        # Configuracion de ciudad, endpoints, sensores e intersecciones
+-- internal/
|   +-- analytics/       # Reglas de congestion y prioridad
|   +-- config/          # Estructuras y loader de configuracion
|   +-- model/           # Eventos, snapshots y estado de ciudad
|   +-- storage/         # SQLite, consultas y failover primary/replica
|   +-- transport/       # Helpers JSON
|   +-- web/             # Frontend embebido del visualizador
+-- start-pc1.sh         # Arranque de broker, sensor-node y visualizer
+-- start-pc2.bat        # Arranque Windows de replica, traffic-light y analytics
+-- start-pc3.sh         # Arranque de primary y monitor
+-- go.mod
+-- go.sum
```

## Requisitos previos

- Go compatible con el modulo del proyecto. El archivo `go.mod` declara:

```text
go 1.26.1
```

- Acceso de red entre los hosts configurados en `configs/city.json`.
- Puertos disponibles para los endpoints ZeroMQ y HTTP definidos en la configuracion.
- En Linux/Unix, para los scripts de arranque con terminales separadas: `gnome-terminal`, `konsole`, `xfce4-terminal` o `xterm`.

## Instalacion

1. Clonar el repositorio.

```bash
git clone <url-del-repositorio>
cd Proy-Distri-Mejora
```

2. Descargar dependencias.

```bash
go mod download
```

3. Verificar compilacion y pruebas.

```bash
go test ./...
go build ./cmd/...
```

## Configuracion

La configuracion principal esta en:

```text
configs/city.json
```

Este archivo define:

- Nombre y dimensiones de la ciudad.
- Tiempos base de semaforo.
- Extension por congestion.
- Duracion de ola verde/prioridad.
- Intervalos de health check.
- Endpoints de comunicacion.
- Intersecciones y relaciones `upstream`.
- Sensores simulados y frecuencia de emision.

### Variables de entorno

| Variable      | Usada por           | Valor por defecto   | Descripcion                                      |
| ------------- | ------------------- | ------------------- | ------------------------------------------------ |
| `CITY_CONFIG` | Todos los servicios | `configs/city.json` | Ruta del archivo de configuracion.               |
| `DB_ROLE`     | `db-server`         | `primary`           | Rol de base de datos: `primary` o `replica`.     |
| `DB_PATH`     | `db-server`         | `<role>.db`         | Ruta del archivo SQLite.                         |
| `AUX`         | `monitor`           | `false`             | Activa modo monitor auxiliar cuando vale `true`. |

### Endpoints utilizados

Definidos en `configs/city.json`:

| Clave                         | Valor actual               | Uso                                    |
| ----------------------------- | -------------------------- | -------------------------------------- |
| `broker_ingest`               | `tcp://10.43.100.155:6001` | Entrada de eventos al broker.          |
| `broker_fanout`               | `tcp://10.43.100.155:6002` | Salida fanout del broker.              |
| `analytics_rep`               | `tcp://10.43.97.128:7001`  | API REQ/REP de analytics para monitor. |
| `traffic_light_pull`          | `tcp://10.43.97.128:7002`  | Entrada de comandos al actuador.       |
| `traffic_light_executed_push` | `tcp://10.43.97.128:7008`  | Comandos ejecutados hacia analytics.   |
| `visualizer_light_push`       | `tcp://10.43.100.155:6003` | Cambios de semaforo hacia visualizer.  |
| `db_primary_pull`             | `tcp://10.43.100.132:7003` | Persistencia hacia DB primario.        |
| `db_replica_pull`             | `tcp://10.43.97.128:7004`  | Persistencia hacia DB replica.         |
| `db_primary_rep`              | `tcp://10.43.100.132:7005` | Consultas REQ/REP al primario.         |
| `db_replica_rep`              | `tcp://10.43.97.128:7006`  | Consultas REQ/REP a la replica.        |
| `visualizer_http`             | `:8080`                    | Servidor web del visualizador.         |

## Ejecucion

### Opcion 1: scripts por maquina

Los scripts estan pensados para distribuir servicios entre varias PCs segun las IPs configuradas.

#### PC1: broker, sensores y visualizador

```bash
./start-pc1.sh
```

Servicios iniciados:

- `broker`
- `sensor-node`
- `visualizer`

UI esperada:

```text
http://10.43.100.155:8080
```

#### PC2: replica, semaforos y analytics

En Windows:

```bat
start-pc2.bat
```

Servicios iniciados:

- `db-server` con `DB_ROLE=replica` y `DB_PATH=replica.db`
- `traffic-light`
- `analytics`

El script elimina `replica.db` antes de iniciar, por lo que esta orientado a ejecuciones limpias de desarrollo/prueba.

#### PC3: primario y monitor

```bash
./start-pc3.sh
```

Servicios iniciados:

- `db-server` con `DB_ROLE=primary` y `DB_PATH=primary.db`
- `monitor`

El script elimina `primary.db` antes de iniciar, por lo que esta orientado a ejecuciones limpias de desarrollo/prueba.

### Opcion 2: ejecucion manual

Compilar binarios:

```bash
go build -o broker ./cmd/broker
go build -o sensor-node ./cmd/sensor-node
go build -o visualizer ./cmd/visualizer
go build -o db-server ./cmd/db-server
go build -o traffic-light ./cmd/traffic-light
go build -o analytics ./cmd/analytics
go build -o monitor ./cmd/monitor
```

Orden recomendado:

```bash
export CITY_CONFIG=configs/city.json

./broker
DB_ROLE=primary DB_PATH=primary.db ./db-server
DB_ROLE=replica DB_PATH=replica.db ./db-server
./traffic-light
./analytics
./visualizer
./sensor-node
./monitor
```

En Windows PowerShell, el formato equivalente es:

```powershell
$env:CITY_CONFIG = "configs\city.json"
$env:DB_ROLE = "primary"
$env:DB_PATH = "primary.db"
.\db-server.exe
```

## Uso

### Visualizador web

El servicio `visualizer` expone:

| Endpoint HTTP | Metodo | Descripcion                                        |
| ------------- | ------ | -------------------------------------------------- |
| `/`           | GET    | Pagina HTML del tablero.                           |
| `/api/state`  | GET    | Estado actual de todas las intersecciones en JSON. |
| `/events`     | GET    | Stream SSE con actualizaciones en tiempo real.     |

Ejemplo:

```bash
curl http://localhost:8080/api/state
```

### Monitor CLI

El monitor puede ejecutarse en modo interactivo:

```bash
./monitor
```

Comandos disponibles:

```text
health
current <intersection>
history
metric_count
force_green <intersection> <duration_sec>
force_green_wave <route>
restore_automatic <intersection>
exit | quit
```

Ejemplos:

```text
current INT_B3
force_green INT_B3 20
force_green_wave B
restore_automatic INT_B3
metric_count
```

Tambien permite una accion inicial por flags:

```bash
./monitor -action current -intersection INT_B3
./monitor -action force_green -intersection INT_B3 -duration 20
./monitor -action force_green_wave -route B
```

### Acciones REQ/REP de base de datos

El `db-server` soporta estas acciones mediante `MonitorRequest`:

| Accion         | Descripcion                                      |
| -------------- | ------------------------------------------------ |
| `health`       | Verifica disponibilidad del servidor de BD.      |
| `current`      | Devuelve el ultimo snapshot de una interseccion. |
| `history`      | Devuelve eventos dentro de un rango temporal.    |
| `metric_count` | Cuenta eventos dentro de un rango temporal.      |

### Acciones REQ/REP de analytics

El servicio `analytics` soporta:

| Accion              | Descripcion                                                      |
| ------------------- | ---------------------------------------------------------------- |
| `health`            | Verifica disponibilidad de analytics.                            |
| `force_green`       | Fuerza una fase verde temporal en una interseccion con semaforo. |
| `force_green_wave`  | Genera una ola verde para una fila o columna.                    |
| `restore_automatic` | Restaura una fase preferida/base para la interseccion.           |

## Modelo de dominio

### Topicos de sensores

| Topico             | Evento           | Datos principales                                   |
| ------------------ | ---------------- | --------------------------------------------------- |
| `sensor.camera`    | `CameraEvent`    | Volumen y velocidad promedio.                       |
| `sensor.gps`       | `GPSEvent`       | Nivel de congestion, densidad y velocidad promedio. |
| `sensor.inductive` | `InductiveEvent` | Vehiculos contados por intervalo.                   |

### Estados de semaforo

```text
VERTICAL_GREEN
HORIZONTAL_GREEN
NONE
```

### Estados de interseccion

El sistema usa cadenas para `status`. Los valores esperados por las reglas y el visualizador incluyen:

```text
NORMAL
CONGESTION
PRIORITY
CONGESTION_BUT_PRIORITY
SOLVED
```

Los eventos GPS simulados tambien generan niveles `NORMAL`, `ALTA` o `BAJA`, pero `analytics` reevalua el estado operacional como `NORMAL` o `CONGESTION`.

## Testing

Existen pruebas unitarias en:

- `internal/analytics/rules_test.go`
- `internal/model/city_test.go`
- `internal/storage/router_test.go`

Ejecutar todas las pruebas:

```bash
go test ./...
```

## Docker

No se detectaron archivos `Dockerfile`, `docker-compose.yml`, manifiestos Kubernetes ni configuracion cloud en el repositorio actual.

## Base de datos

El proyecto utiliza SQLite embebido mediante `modernc.org/sqlite`.

Tablas creadas automaticamente por `internal/storage/sqlite.go`:

### `traffic_events`

Almacena snapshots, eventos persistidos y metadatos de trazabilidad.

Campos principales:

- `event_id`
- `kind`
- `topic`
- `intersection`
- `has_semaphore`
- `status`
- `light_state`
- `queue_length`
- `avg_speed`
- `density`
- `raw_payload`
- `created_at`

### `light_actions`

Almacena comandos de semaforo ejecutados o registrados.

Campos principales:

- `command_id`
- `intersection`
- `target_state`
- `duration_sec`
- `reason`
- `requested_by`
- `requested_at`
- `changed_at`

## Scripts utiles

| Script          | Plataforma | Descripcion                                                          |
| --------------- | ---------- | -------------------------------------------------------------------- |
| `start-pc1.sh`  | Linux/Unix | Compila e inicia `broker`, `sensor-node` y `visualizer`.             |
| `start-pc2.bat` | Windows    | Compila e inicia `db-server` replica, `traffic-light` y `analytics`. |
| `start-pc3.sh`  | Linux/Unix | Compila e inicia `db-server` primario y `monitor`.                   |

Comandos utiles adicionales:

```bash
go test ./...
go build ./cmd/...
go run ./cmd/visualizer
go run ./cmd/monitor -action current -intersection INT_B3
```

## Estado del proyecto

Por su estructura, scripts por PC, simuladores de sensores e IPs privadas en la configuracion, el proyecto parece estar en estado academico/en desarrollo para una practica de sistemas distribuidos. No se detectan configuraciones de despliegue productivo, CI/CD, Docker ni gestion externa de secretos.

## Posibles mejoras futuras

- Externalizar `configs/city.json` por ambiente y evitar IPs fijas versionadas.
- Agregar un `.env.example` o documentacion de perfiles de ejecucion local.
- Incorporar Docker Compose para levantar todo el sistema en una sola maquina.
- Agregar CI para ejecutar `go test ./...` y `go build ./cmd/...`.
- Tipar `status`, `reason` y fases de semaforo como constantes compartidas o enums de dominio.
- Mejorar observabilidad con logs estructurados, metricas y trazas de correlacion.
- Agregar migraciones versionadas para SQLite.
- Documentar contratos JSON con ejemplos completos de payloads.
- Separar configuracion local de configuracion multi-PC.
- Agregar autenticacion/autorizacion si el monitor se expone fuera de una red controlada.

## Autores

Autor inferido desde el historial Git:

- Angel Morales
- Luz Salazar
- Guden Silva
