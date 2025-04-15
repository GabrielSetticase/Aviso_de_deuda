# Sistema de Aviso de Deuda

Este sistema automatiza el envío de notificaciones por correo electrónico y WhatsApp para recordar a los clientes sobre sus vencimientos de deuda próximos.

## Requisitos

- Python 3.x
- Microsoft Access Driver (*.mdb, *.accdb)
- Base de datos de actas (cor*.mdb)
- Base de datos de empresas (4- EMPRESAS CORDOBA.mdb)

## Configuración

1. Instalar las dependencias:
   ```
   pip install -r requirements.txt
   ```

2. Crear un archivo `.env` con las siguientes variables:
   ```
   EMAIL_SENDER=tu_email@gmail.com
   EMAIL_PASSWORD=tu_contraseña_de_aplicacion
   ```
   Nota: Para Gmail, debes usar una contraseña de aplicación.

## Uso

1. Asegúrate de tener las bases de datos necesarias en el directorio del proyecto:
   - Base de datos de actas (cor*.mdb)
   - Base de datos de empresas (4- EMPRESAS CORDOBA.mdb)

2. Ejecutar el sistema:
   ```
   python main.py
   ```
   O usar el archivo batch:
   ```
   iniciar_sistema.bat
   ```

El sistema se ejecutará automáticamente todos los días a las 09:00 y enviará notificaciones a los clientes que tengan vencimientos en 2 días.

## Configuración del Inicio Automático

Para configurar el sistema para que se inicie automáticamente cuando enciendas la computadora:

1. Abre PowerShell como administrador
2. Navega hasta la carpeta del proyecto
3. Ejecuta el script de configuración:
   ```
   .\configurar_tarea.ps1
   ```

Esto creará una tarea programada llamada "SistemaAvisoDeuda" que ejecutará el sistema cada vez que inicies Windows.

Para deshabilitar el inicio automático:
1. Abre el Programador de tareas de Windows
2. Busca la tarea "SistemaAvisoDeuda"
3. Haz clic derecho y selecciona "Deshabilitar" o "Eliminar"

## Registro de Notificaciones

Todas las notificaciones enviadas se registran en el archivo `notificaciones.csv` con la siguiente información:
- Fecha y hora del envío
- Tipo de notificación (Email/WhatsApp)
- Número de acta
- Destinatario
- Estado del envío
- Detalles adicionales