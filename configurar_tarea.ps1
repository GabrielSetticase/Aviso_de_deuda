# Configurar tarea programada para ejecutar el sistema al inicio y diariamente
$scriptPath = Join-Path $PSScriptRoot "iniciar_sistema.bat"
$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$scriptPath`""

# Crear dos triggers: uno al inicio y otro diario
$TriggerDaily = New-ScheduledTaskTrigger -Daily -At 9:00AM
$TriggerStartup = New-ScheduledTaskTrigger -AtStartup

$Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 365)

# Registrar la tarea con ambos triggers
Register-ScheduledTask -TaskName "SistemaAvisoDeuda" -Action $Action -Principal $Principal -Settings $Settings -Trigger $TriggerDaily,$TriggerStartup -Force

Write-Host "Tarea programada configurada exitosamente. El sistema se iniciará automáticamente al encender la computadora."