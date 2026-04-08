@echo off
title Knowledge Repository
cd /d "%~dp0"
echo Starting Knowledge Repository on http://localhost:8000
echo.
python -m uvicorn ingestion.api.app:app --host 0.0.0.0 --port 8000 --reload
pause
