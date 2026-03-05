@echo off
REM TrailOps - Tray launcher (windowless)
cd /d C:\trail_ops
REM No console: pythonw runs without a terminal window
C:\trail_ops\.venv\Scripts\pythonw.exe -m app.tools.tray_streamlit
