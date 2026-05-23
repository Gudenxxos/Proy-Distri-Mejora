#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "========================================"
echo "Starting PC1 services from \"$(pwd)\""
echo "PC1 IP: 10.43.100.155"
echo "Services: broker, sensor-node, visualizer"
echo "========================================"
echo

echo "[1/3] Compiling broker..."
go build -o broker ./cmd/broker
echo "[OK] broker compiled successfully"
echo

echo "[2/3] Compiling sensor-node..."
go build -o sensor-node ./cmd/sensor-node
echo "[OK] sensor-node compiled successfully"
echo

echo "[3/3] Compiling visualizer..."
go build -o visualizer ./cmd/visualizer
echo "[OK] visualizer compiled successfully"
echo

export CITY_CONFIG=configs/city.json

open_service_terminal() {
	local title="$1"
	local service_command="$2"
	local shell_command
	shell_command="cd \"$(pwd)\" && export CITY_CONFIG=configs/city.json && stdbuf -oL -eL $service_command; service_exit=\$?; echo; echo \"[$title] exited with status \$service_exit\"; read -r -p \"Press Enter to close this terminal...\" _"

	if command -v gnome-terminal >/dev/null 2>&1; then
		gnome-terminal --title="$title" -- bash -lc "$shell_command" &
		return 0
	fi

	if command -v konsole >/dev/null 2>&1; then
		konsole --new-tab -p tabtitle="$title" -e bash -lc "$shell_command" &
		return 0
	fi

	if command -v xfce4-terminal >/dev/null 2>&1; then
		xfce4-terminal --title="$title" -- bash -lc "$shell_command" &
		return 0
	fi

	if command -v xterm >/dev/null 2>&1; then
		xterm -T "$title" -e bash -lc "$shell_command" &
		return 0
	fi

	echo "[WARN] No supported terminal emulator found. Running $title in this terminal instead."
	bash -lc "$shell_command" &
}

echo "========================================"
echo "All compilations successful! Starting services..."
echo "========================================"
echo

open_service_terminal "broker" "./broker"
sleep 1

open_service_terminal "sensor-node" "./sensor-node"
sleep 1

open_service_terminal "visualizer" "./visualizer"

echo
echo "PC1 launch complete."
echo "Open the UI at: http://10.43.100.155:8080"