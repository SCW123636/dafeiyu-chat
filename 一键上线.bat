@echo off
chcp 65001 >nul
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "startup\dafeiyu_online.ps1"
powershell -NoProfile -Command "Get-Content -Encoding UTF8 'startup\online.log' -Tail 6"
pause
