@echo off
echo ==========================================
echo   MAPS SPIDER DASHBOARD - LAUNCHER
echo ==========================================

:: Start the Python API in a new window
echo Starting Backend (FastAPI)...
start "Maps Spider API" cmd /k "python api.py"

:: Start the React Dashboard
echo Starting Frontend (Vite)...
cd dashboard
start http://localhost:5175
npm run dev

pause
