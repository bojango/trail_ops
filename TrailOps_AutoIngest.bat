@echo off
REM TrailOps - Auto ingest (no pause). Safe for Task Scheduler / hidden launchers.
cd /d C:\trail_ops
call .venv\Scripts\activate

python -m app.tools.auto_ingest_once
