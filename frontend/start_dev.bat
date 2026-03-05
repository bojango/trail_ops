@echo off
REM TrailOps React dashboard (dev server)
cd /d C:\trail_ops\frontend

REM Install dependencies once
if not exist node_modules (
  echo [TrailOps UI] Installing npm dependencies...
  npm install
)

echo [TrailOps UI] Starting dev server on http://127.0.0.1:5173
npm run dev
