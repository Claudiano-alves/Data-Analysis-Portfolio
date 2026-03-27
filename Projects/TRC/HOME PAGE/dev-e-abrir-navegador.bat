@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo [1/3] npm install...
call npm install
if errorlevel 1 (
  echo Falha no npm install.
  pause
  exit /b 1
)

echo [2/3] Iniciando servidor Vite (nova janela)...
start "Vite - Portal MIS" cmd /k "cd /d "%~dp0" && npm run dev -- --host 127.0.0.1 --port 5173"

echo [3/3] Aguardando o servidor subir...
timeout /t 5 /nobreak >nul

echo Abrindo http://127.0.0.1:5173 ...
start "" "http://127.0.0.1:5173/"

echo.
echo Use a janela "Vite - Portal MIS" para ver o log. Feche-a para encerrar o servidor.
pause
endlocal
