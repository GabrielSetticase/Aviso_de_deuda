# Configurar tarea programada para ejecutar el sistema al inicio
$scriptPath = Join-Path $PSScriptRoot "iniciar_sistema.bat"
$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$scriptPath`""
$Trigger = New-ScheduledTaskTrigger -AtStartup
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 365)

# Registrar la tarea
Register-ScheduledTask -TaskName "SistemaAvisoDeuda" -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings -Force

Write-Host "Tarea programada configurada exitosamente. El sistema se iniciará automáticamente al encender la computadora."