import os
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pywhatkit
from dotenv import load_dotenv
import schedule
import time
import csv

class NotificationSystem:
    def __init__(self):
        load_dotenv()
        self.email_sender = os.getenv('EMAIL_SENDER')
        self.email_password = os.getenv('EMAIL_PASSWORD')
        self.notification_log = set()
        self.log_file = 'notificaciones.csv'
        self.initialize_log_file()
        self.message_template = """
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; margin: 20px;">
            <div style="color: #333; margin-bottom: 20px;">
                <p>Estimado/a {razon_social}, {cuit}</p>
            </div>
            
            <div style="margin-bottom: 15px;">
                <p>Le recordamos que tiene un vencimiento próximo:</p>
                <p><strong>Acta:</strong> {acta}<br>
                <strong>Fecha de Vencimiento:</strong> {vencimiento}<br>
                <strong>Total a pagar:</strong> ${total}</p>
            </div>
            
            <div style="margin-bottom: 15px;">
                <p>Por favor, comuníquese con el inspector asignado o con la Administración para regularizar su situación.</p>
            </div>
            
            <div style="background-color: #f5f5f5; padding: 10px; margin: 15px 0;">
                <p>Whatsapp de la Administración: <strong>(+543513875875)</strong> sólo mensajes, no se atienden llamadas.</p>
            </div>
            
            <p>Saludos cordiales.</p>
            
            <div style="color: #666; font-size: 0.9em; border-top: 1px solid #eee; padding-top: 15px;">
                <p style="color: #d32f2f; font-weight: bold;">*** ESTE ES UN MENSAJE AUTOMÁTICO Y NO REQUIERE RESPUESTA ***</p>
                <p style="color: #d32f2f; font-weight: bold;">*** SI HA CANCELADO EL ACTA (POSEE RECIBO OFICIAL DE OSECAC)
                O ESTÁ GESTIONANDO EL PAGO CON EL INSPECTOR ASIGNADO,
                POR FAVOR DESESTIME ESTE MENSAJE ***</p>
            </div>
        </body>
        </html>
        """
        self.overdue_template = """
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; margin: 20px;">
            <div style="color: #333; margin-bottom: 20px;">
                <p>Estimado/a {razon_social}, {cuit}</p>
            </div>
            
            <div style="margin-bottom: 15px;">
                <p>Le recordamos que el acta de inspección Nº {acta}, se encuentra vencida el día {vencimiento} incurriendo en mora.</p>
            </div>
            
            <div style="margin-bottom: 15px;">
                <p>Por favor, comuníquese con el inspector asignado o con la Administración para consultar el monto actualizado y regularizar su situación.</p>
            </div>
            
            <div style="background-color: #f5f5f5; padding: 10px; margin: 15px 0;">
                <p>Whatsapp de la Administración: <strong>(+543513875875)</strong> sólo mensajes, no se atienden llamadas.</p>
            </div>
            
            <div style="background-color: #fff3e0; padding: 15px; margin: 15px 0; border-left: 4px solid #ff9800;">
                <p><strong>IMPORTANTE: Pasados 60 (sesenta) días del vencimiento, se iniciará la gestión de cobro extra judicial por parte del Departamento Legales de esta Delegación.</strong></p>
            </div>
            
            <p>Saludos cordiales.</p>
            
            <div style="color: #666; font-size: 0.9em; border-top: 1px solid #eee; padding-top: 15px;">
                <p style="color: #d32f2f; font-weight: bold;">*** ESTE ES UN MENSAJE AUTOMÁTICO Y NO REQUIERE RESPUESTA ***</p>
                <p style="color: #d32f2f; font-weight: bold;">*** SI HA CANCELADO EL ACTA (POSEE RECIBO OFICIAL DE OSECAC)
                O ESTÁ GESTIONANDO EL PAGO CON EL INSPECTOR ASIGNADO,
                POR FAVOR DESESTIME ESTE MENSAJE ***</p>
            </div>
        </body>
        </html>
        """

    def load_mdb_data(self):
        try:
            # Buscar todos los archivos cor*.mdb
            mdb_files = [f for f in os.listdir() if f.startswith('cor') and f.endswith('.mdb')]
            if not mdb_files:
                print("No se encontraron archivos cor*.mdb")
                return None
            
            # Lista para almacenar los DataFrames de cada archivo
            all_actas = []
            
            # Procesar cada archivo cor*.mdb
            for mdb_file in mdb_files:
                try:
                    print(f"Procesando archivo: {mdb_file}")
                    conn_str = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={os.path.abspath(mdb_file)}'
                    conn = pyodbc.connect(conn_str)
                    df = pd.read_sql('SELECT NRO_ACTA, RAZON_SOCIAL, FECHA_PAGO_OBL, TOTALDEUDAACTUALIZADA, CUIT FROM actas', conn)
                    conn.close()
                    all_actas.append(df)
                except Exception as e:
                    print(f"Error al procesar {mdb_file}: {e}")
                    continue
            
            if not all_actas:
                print("No se pudo procesar ningún archivo cor*.mdb")
                return None
                
            # Combinar todos los DataFrames
            actas_df = pd.concat(all_actas, ignore_index=True)
            
            # Conectar a la base de datos de empresas
            empresas_db = '4- EMPRESAS CORDOBA.mdb'
            if not os.path.exists(empresas_db):
                print(f"No se encontró el archivo {empresas_db}")
                return None
                
            conn_str = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={os.path.abspath(empresas_db)}'
            conn = pyodbc.connect(conn_str)
            empresas_df = pd.read_sql('SELECT CUIT, EMAIL as MAIL, TEL_DOM_LEGAL, TEL_DOM_REAL FROM vw_EmpresasInterior', conn)
            conn.close()
            
            # Combinar los dataframes usando el CUIT
            df = pd.merge(actas_df, empresas_df, on='CUIT', how='left')
            
            # Renombrar las columnas para mantener compatibilidad con el código existente
            df = df.rename(columns={
                'NRO_ACTA': 'ACTA',
                'RAZON_SOCIAL': 'RAZON SOCIAL',
                'FECHA_PAGO_OBL': 'VENCIMIENTO',
                'TOTALDEUDAACTUALIZADA': 'TOTAL ACTA'
            })
            
            return df
        except Exception as e:
            print(f"Error al cargar los datos de las bases: {e}")
            return None

    def adjust_business_day(self, date, direction='backward'):
        # Si es fin de semana, ajustar al viernes anterior o lunes siguiente
        while date.weekday() >= 5:  # 5 = Sábado, 6 = Domingo
            if direction == 'backward':
                date -= timedelta(days=1)
            else:
                date += timedelta(days=1)
        return date

    def check_upcoming_due_dates(self, df):
        today = datetime.now().date()
        
        for _, row in df.iterrows():
            vencimiento = pd.to_datetime(row['VENCIMIENTO']).date()
            notification_key = f"{row['ACTA']}_{vencimiento}"
            overdue_key = f"{row['ACTA']}_{vencimiento}_overdue"
            
            # Calcular la fecha de notificación (2 días antes)
            notification_date = vencimiento - timedelta(days=2)
            notification_date = self.adjust_business_day(notification_date, 'backward')
            
            # Calcular la fecha de aviso de mora (10 días después)
            overdue_date = vencimiento + timedelta(days=10)
            overdue_date = self.adjust_business_day(overdue_date, 'backward')
            
            # Verificar notificaciones de vencimiento próximo
            if today == notification_date and notification_key not in self.notification_log:
                self.send_notifications(row, is_overdue=False)
                self.notification_log.add(notification_key)
            
            # Verificar notificaciones de actas vencidas
            if today == overdue_date and overdue_key not in self.notification_log:
                self.send_notifications(row, is_overdue=True)
                self.notification_log.add(overdue_key)

    def initialize_log_file(self):
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Fecha', 'Tipo', 'Acta', 'Destinatario', 'Estado', 'Detalle'])

    def log_notification(self, notification_type, acta, destinatario, estado, detalle=''):
        with open(self.log_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           notification_type,
                           acta,
                           destinatario,
                           estado,
                           detalle])

    def send_email(self, row, is_overdue=False):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_sender
            msg['To'] = row['MAIL']
            
            if is_overdue:
                msg['Subject'] = f"Aviso de acta {row['ACTA']} impaga {row['RAZON SOCIAL']}"
                message = self.overdue_template.format(
                    acta=row['ACTA'],
                    cuit=row['CUIT'],
                    razon_social=row['RAZON SOCIAL'],
                    vencimiento=row['VENCIMIENTO'].strftime('%d/%m/%Y')
                )
            else:
                msg['Subject'] = f"Aviso vencimiento de deuda - Acta {row['ACTA']} {row['RAZON SOCIAL']}"
                message = self.message_template.format(
                    acta=row['ACTA'],
                    cuit=row['CUIT'],
                    razon_social=row['RAZON SOCIAL'],
                    vencimiento=row['VENCIMIENTO'].strftime('%d/%m/%Y'),
                    total=row['TOTAL ACTA']
                )
            
            msg.attach(MIMEText(message, 'html'))
            
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.email_sender, self.email_password)
                server.send_message(msg)
            
            print(f"Email enviado a {row['MAIL']}")
            self.log_notification('Email', row['ACTA'], row['MAIL'], 'Enviado')
        except Exception as e:
            error_msg = str(e)
            print(f"Error al enviar email: {error_msg}")
            self.log_notification('Email', row['ACTA'], row['MAIL'], 'Error', error_msg)

    def is_whatsapp_web_open(self):
        try:
            import psutil
            import time
            
            # Verificar si hay un navegador abierto
            browser_processes = ['chrome.exe', 'msedge.exe', 'firefox.exe', 'opera.exe']
            browser_open = False
            
            for proc in psutil.process_iter(['name']):
                if any(browser in proc.info['name'].lower() for browser in browser_processes):
                    browser_open = True
                    break
            
            if not browser_open:
                print("No se detectó ningún navegador abierto. WhatsApp Web no está disponible.")
                return False
            
            # Dar tiempo para que WhatsApp Web esté completamente cargado
            time.sleep(5)  # Aumentado a 5 segundos para mayor seguridad
            
            print("Se detectó navegador abierto. Asumiendo que WhatsApp Web está disponible.")
            return True
            
        except Exception as e:
            print(f"Error al verificar WhatsApp Web: {str(e)}")
            return False

    def send_whatsapp(self, row, is_overdue=False):
        try:
            # Verificar si WhatsApp Web está disponible
            if not self.is_whatsapp_web_open():
                print("WhatsApp Web no está disponible. Se omitirá el envío de mensajes por WhatsApp.")
                self.log_notification('WhatsApp', row['ACTA'], 'múltiples números', 'Omitido', 'WhatsApp Web no está disponible')
                return

            if is_overdue:
                message = f"Estimado/a {row['RAZON SOCIAL']}, {row['CUIT']}\n\n"
                message += f"Le recordamos que el acta de inspección Nº {row['ACTA']}, se encuentra vencida el día {row['VENCIMIENTO'].strftime('%d/%m/%Y')} incurriendo en mora.\n\n"
                message += "Por favor, comuníquese con el inspector asignado o con la Administración para consultar el monto actualizado y regularizar su situación.\n\n"
                message += "Whatsapp de la Administración: (+543513875875) sólo mensajes, no se atienden llamadas.\n\n"
                message += "IMPORTANTE: Pasados 60 (sesenta) días del vencimiento, se iniciará la gestión de cobro extra judicial por parte del Departamento Legales de esta Delegación.\n\n"
                message += "Saludos cordiales.\n\n"
                message += "*** ESTE ES UN MENSAJE AUTOMÁTICO Y NO REQUIERE RESPUESTA ***\n"
                message += "*** SI HA CANCELADO EL ACTA (POSEE RECIBO OFICIAL DE OSECAC) O ESTÁ GESTIONANDO CON EL PAGO CON EL INSPECTOR ASIGNADO, POR FAVOR DESESTIME ESTE MENSAJE ***"
            else:
                message = f"Estimado/a {row['RAZON SOCIAL']}, {row['CUIT']}\n\n"
                message += "Le recordamos que tiene un vencimiento próximo:\n"
                message += f"Acta: {row['ACTA']}\n"
                message += f"Fecha de Vencimiento: {row['VENCIMIENTO'].strftime('%d/%m/%Y')}\n"
                message += f"Total a pagar: ${row['TOTAL ACTA']}\n\n"
                message += "Por favor, comuníquese con el inspector asignado o con la Administración para regularizar su situación.\n\n"
                message += "Whatsapp de la Administración: (+543513875875) sólo mensajes, no se atienden llamadas.\n\n"
                message += "Saludos cordiales.\n\n"
                message += "*** ESTE ES UN MENSAJE AUTOMÁTICO Y NO REQUIERE RESPUESTA ***\n"
                message += "*** SI HA CANCELADO EL ACTA (POSEE RECIBO OFICIAL DE OSECAC) O ESTÁ GESTIONANDO CON EL PAGO CON EL INSPECTOR ASIGNADO, POR FAVOR DESESTIME ESTE MENSAJE ***"
            
            # Función para formatear y enviar a un número
            def send_to_number(phone_number, delay_minutes=0):
                try:
                    if pd.notna(phone_number):
                        phone_number = str(phone_number)
                        # Limpiar el número de teléfono
                        phone_number = ''.join(filter(str.isdigit, phone_number))
                        
                        # Formatear el número para WhatsApp
                        if phone_number.startswith('0'):
                            phone_number = '+54' + phone_number[1:]
                        elif phone_number.startswith('54'):
                            phone_number = '+' + phone_number
                        else:
                            phone_number = '+54' + phone_number
                        
                        # Verificar longitud del número
                        if len(phone_number) < 12:
                            raise ValueError(f"Número de teléfono inválido: {phone_number}")
                        
                        now = datetime.now()
                        send_time = now + timedelta(minutes=2 + delay_minutes)
                        
                        print(f"Intentando enviar WhatsApp a {phone_number}")
                        pywhatkit.sendwhatmsg(phone_number, message, 
                                             send_time.hour, 
                                             send_time.minute)
                        
                        print(f"WhatsApp enviado exitosamente a {phone_number}")
                        self.log_notification('WhatsApp', row['ACTA'], phone_number, 'Enviado')
                except Exception as e:
                    error_msg = f"Error al enviar WhatsApp a {phone_number}: {str(e)}"
                    print(error_msg)
                    self.log_notification('WhatsApp', row['ACTA'], phone_number, 'Error', error_msg)
            
            # Enviar a ambos números con retraso entre ellos
            if pd.notna(row['TEL_DOM_LEGAL']):
                send_to_number(row['TEL_DOM_LEGAL'])
            if pd.notna(row['TEL_DOM_REAL']):
                send_to_number(row['TEL_DOM_REAL'], delay_minutes=1)
            
        except Exception as e:
            error_msg = f"Error general en send_whatsapp: {str(e)}"
            print(error_msg)
            self.log_notification('WhatsApp', row['ACTA'], 'múltiples números', 'Error', error_msg)

    def send_notifications(self, row, is_overdue=False):
        if pd.notna(row['MAIL']):
            self.send_email(row, is_overdue)
        if pd.notna(row['TEL_DOM_LEGAL']) or pd.notna(row['TEL_DOM_REAL']):
            self.send_whatsapp(row, is_overdue)

    def check_pending_notifications(self, df):
        today = datetime.now().date()
        sent_notifications = set()
        
        # Cargar notificaciones ya enviadas del CSV
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Saltar encabezado
                for row in reader:
                    if len(row) >= 3:  # Asegurar que la fila tiene suficientes columnas
                        sent_notifications.add(f"{row[2]}_{row[0].split()[0]}")  # Acta_Fecha
        
        # Revisar notificaciones pendientes de los últimos 7 días
        for _, row in df.iterrows():
            vencimiento = pd.to_datetime(row['VENCIMIENTO']).date()
            
            # Calcular fechas de notificación y mora
            notification_date = vencimiento - timedelta(days=2)
            notification_date = self.adjust_business_day(notification_date, 'backward')
            
            overdue_date = vencimiento + timedelta(days=10)
            overdue_date = self.adjust_business_day(overdue_date, 'backward')
            
            # Verificar notificaciones pendientes de los últimos 7 días
            for check_date in [(notification_date, False), (overdue_date, True)]:
                date_to_check, is_overdue = check_date
                notification_key = f"{row['ACTA']}_{date_to_check}"
                
                # Si la fecha está dentro de los últimos 7 días y la notificación no fue enviada
                if (today - date_to_check).days <= 7 and (today - date_to_check).days >= 0:
                    if notification_key not in sent_notifications and notification_key not in self.notification_log:
                        print(f"Enviando notificación pendiente para acta {row['ACTA']} del {date_to_check}")
                        self.send_notifications(row, is_overdue)
                        self.notification_log.add(notification_key)

    def check_mdb_files(self):
        df = self.load_mdb_data()
        if df is not None:
            self.check_pending_notifications(df)  # Verificar notificaciones pendientes
            self.check_upcoming_due_dates(df)  # Verificar notificaciones del día actual

def main():
    notification_system = NotificationSystem()
    print("Sistema de notificaciones iniciado. Las notificaciones se registrarán en 'notificaciones.csv'")
    print("El sistema se ejecutará automáticamente todos los días a las 09:00")
    print("Para detener el programa, presione Ctrl+C")
    
    # Programar la verificación diaria
    schedule.every().day.at("09:00").do(notification_system.check_mdb_files)
    
    # Mantener el programa en ejecución
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()