#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "========================================"
echo "Starting PC3 services from \"$(pwd)\""
echo "PC3 IP: 10.43.100.132"
echo "Services: db-primary, monitor"
echo "========================================"
echo

rm -f primary.db

echo

echo "[1/2] Compiling db-server..."
go build -o db-server ./cmd/db-server
echo "[OK] db-server compiled successfully"
echo

echo "[2/2] Compiling monitor..."
go build -o monitor ./cmd/monitor
echo "[OK] monitor compiled successfully"
echo

export CITY_CONFIG=configs/city.json

open_service_terminal() {
	local title="$1"
	local service_command="$2"
	local shell_command
	shell_command="cd \"$(pwd)\" && export CITY_CONFIG=configs/city.json && eval \"stdbuf -oL -eL $service_command\"; service_exit=\$?; echo; echo \"[$title] exited with status \$service_exit\"; read -r -p \"Press Enter to close this terminal...\" _"

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

	echo "[WARN] No supported terminal emulator found. Running $title in this terminal instead."
	bash -lc "$shell_command" &
}

echo "========================================"
echo "All compilations successful! Starting services..."
echo "========================================"
echo

open_service_terminal "PC3 - DB Primary" "bash -c 'export DB_ROLE=primary DB_PATH=primary.db && ./db-server'"
sleep 1

open_service_terminal "PC3 - Monitor Service" "./monitor"

echo
echo "PC3 launch complete."
echo "Services launched in separate terminals:"
echo "  - db-server (primary role)"
echo "  - monitor (ready for queries)"
echo "========================================"
