@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop_service.ps1" %*
