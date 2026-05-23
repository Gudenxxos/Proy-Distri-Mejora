@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo ========================================
echo Starting PC2 services from "%cd%"
echo PC2 IP: 172.20.10.8
echo Services: db-replica, traffic-light, analytics
echo ========================================
echo.

echo.
echo [1/3] Compiling db-server...
go build -o db-server.exe ./cmd/db-server
if errorlevel 1 (
    echo ERROR: Failed to compile db-server
    pause
    exit /b 1
)
echo [✓] db-server compiled successfully

echo.
echo [2/3] Compiling traffic-light...
go build -o traffic-light.exe ./cmd/traffic-light
if errorlevel 1 (
    echo ERROR: Failed to compile traffic-light
    pause
    exit /b 1
)
echo [✓] traffic-light compiled successfully

echo.
echo [3/3] Compiling analytics...
go build -o analytics.exe ./cmd/analytics
if errorlevel 1 (
    echo ERROR: Failed to compile analytics
    pause
    exit /b 1
)
echo [✓] analytics compiled successfully

echo.
echo ========================================
echo All compilations successful! Starting services...
echo ========================================
echo.

set "CITY_CONFIG=configs\city.json"
set "DB_ROLE=replica"
set "DB_PATH=replica.db"
start "db-replica" cmd /v:on /k "cd /d ""%~dp0"" && echo DB_ROLE=[!DB_ROLE!] DB_PATH=[!DB_PATH!] && db-server.exe"
timeout /t 1 /nobreak >nul

set "DB_ROLE="
set "DB_PATH="
start "traffic-light" cmd /k "cd /d ""%~dp0"" && traffic-light.exe"
timeout /t 1 /nobreak >nul

start "analytics" cmd /k "cd /d ""%~dp0"" && analytics.exe"

echo.
echo PC2 launch complete.
echo Services running: db-replica (replica role), traffic-light, analytics
pause