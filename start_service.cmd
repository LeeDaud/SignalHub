@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_service.ps1" %*
