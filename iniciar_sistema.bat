@echo off
echo Iniciando sistema de notificaciones...
cd /d "%~dp0"
powershell -Command "Start-Process pythonw -ArgumentList 'main.py' -Verb RunAs -WindowStyle Hidden -Wait"