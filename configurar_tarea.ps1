# Verificar si se está ejecutando como administrador
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    try {
        # Reiniciar el script con privilegios de administrador
        Start-Process powershell.exe -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    } catch {
        Write-Host "Error al intentar ejecutar como administrador: $($_.Exception.Message)" -ForegroundColor Red
    }
    exit
}

# Configurar tarea programada para ejecutar el sistema diariamente a las 9:00 AM
try {
    $scriptPath = Join-Path $PSScriptRoot "iniciar_sistema.bat"
    $Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$scriptPath`""

    # Crear trigger diario para las 9:00 AM
    $TriggerDaily = New-ScheduledTaskTrigger -Daily -At 9:00AM

    # Configurar el principal para ejecutar como SYSTEM con privilegios elevados
    $Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

    # Configurar las opciones de la tarea
    $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 1)

    # Eliminar la tarea existente si existe
    Unregister-ScheduledTask -TaskName "SistemaAvisoDeuda" -Confirm:$false -ErrorAction SilentlyContinue

    # Registrar la nueva tarea
    $Task = Register-ScheduledTask -TaskName "SistemaAvisoDeuda" -Action $Action -Principal $Principal -Settings $Settings -Trigger $TriggerDaily -Force

    # Obtener y mostrar la información de la tarea
    $TaskInfo = Get-ScheduledTaskInfo -TaskName "SistemaAvisoDeuda"
    Write-Host "Tarea programada configurada exitosamente:" -ForegroundColor Green
    Write-Host "Última ejecución: $($TaskInfo.LastRunTime)"
    Write-Host "Próxima ejecución: $($TaskInfo.NextRunTime)"
    Write-Host "El sistema se ejecutará automáticamente todos los días a las 9:00 AM."
} catch {
    Write-Host "Error al configurar la tarea programada: $($_.Exception.Message)" -ForegroundColor Red
}

# Pausar para ver los mensajes
Write-Host "
Presione cualquier tecla para continuar..."
$null = $Host.UI.RawUI.ReadKey('NoEcho,IncludeKeyDown')