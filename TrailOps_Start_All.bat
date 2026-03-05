@echo off
REM TrailOps - Start Everything (Tray + API + React UI)
REM This launches:
REM  - FastAPI (http://127.0.0.1:8000)
REM  - React dev UI (http://127.0.0.1:5173) if Node is installed
REM  - Streamlit tray controller (pythonw) which manages the Streamlit server

cd /d C:\trail_ops

REM ---- Start FastAPI in its own console window ----
start "TrailOps API" cmd /k "cd /d C:\trail_ops && call .venv\Scripts\activate && python -m app.tools.run_api"

REM ---- Start React dev server (optional) ----
where node >nul 2>nul
if %errorlevel%==0 (
    where npm >nul 2>nul
    if %errorlevel%==0 (
        start "TrailOps Dashboard UI" cmd /k "cd /d C:\trail_ops\frontend && call start_dev.bat"
    ) else (
        echo [TrailOps] npm not found. React dashboard will not start.
    )
) else (
    echo [TrailOps] Node.js not found. React dashboard will not start.
)

REM ---- Start the tray app (windowless) ----
REM This uses pythonw so it should not create a terminal window.
REM We still launch it via a minimized cmd to avoid blocking this script.
start "" /min cmd /c call C:\trail_ops\TrailOps_Tray_Start.bat

echo [TrailOps] Launched API, UI (if available), and Tray.
echo [TrailOps] If API says port 8000 is already in use, it is already running.
pause
