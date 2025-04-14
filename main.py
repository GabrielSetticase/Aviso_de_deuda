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
        Estimado cliente,
        
        Le recordamos que tiene un vencimiento próximo:
        Acta: {acta}
        CUIT: {cuit}
        Razón Social: {razon_social}
        Fecha de Vencimiento: {vencimiento}
        Total a pagar: ${total}
        
        Por favor, realice el pago correspondiente.
        Saludos cordiales.
        """

    def load_mdb_data(self):
        try:
            # Buscar el archivo cor*.mdb más reciente
            mdb_files = [f for f in os.listdir() if f.startswith('cor') and f.endswith('.mdb')]
            if not mdb_files:
                print("No se encontraron archivos cor*.mdb")
                return None
            
            latest_mdb = max(mdb_files)
            
            # Conectar a la base de datos de actas
            conn_str = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={os.path.abspath(latest_mdb)}'
            conn = pyodbc.connect(conn_str)
            actas_df = pd.read_sql('SELECT NRO_ACTA, RAZON_SOCIAL, FECHA_PAGO_OBL, TOTALDEUDAACTUALIZADA, CUIT FROM actas', conn)
            conn.close()
            
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

    def check_upcoming_due_dates(self, df):
        today = datetime.now().date()
        two_days_from_now = today + timedelta(days=2)
        
        for _, row in df.iterrows():
            vencimiento = pd.to_datetime(row['VENCIMIENTO']).date()
            notification_key = f"{row['ACTA']}_{vencimiento}"
            
            if vencimiento == two_days_from_now and notification_key not in self.notification_log:
                self.send_notifications(row)
                self.notification_log.add(notification_key)

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

    def send_email(self, row):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_sender
            msg['To'] = row['MAIL']
            msg['Subject'] = f"Recordatorio de Vencimiento - Acta {row['ACTA']}"
            
            message = self.message_template.format(
                acta=row['ACTA'],
                cuit=row['CUIT'],
                razon_social=row['RAZON SOCIAL'],
                vencimiento=row['VENCIMIENTO'].strftime('%d/%m/%Y'),
                total=row['TOTAL ACTA']
            )
            
            msg.attach(MIMEText(message, 'plain'))
            
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

    def send_whatsapp(self, row):
        try:
            message = self.message_template.format(
                acta=row['ACTA'],
                cuit=row['CUIT'],
                razon_social=row['RAZON SOCIAL'],
                vencimiento=row['VENCIMIENTO'].strftime('%d/%m/%Y'),
                total=row['TOTAL ACTA']
            )
            
            # Función para formatear y enviar a un número
            def send_to_number(phone_number, delay_minutes=0):
                if pd.notna(phone_number):
                    phone_number = str(phone_number)
                    if phone_number.startswith('0'):
                        phone_number = '54' + phone_number[1:]
                    elif not phone_number.startswith('54'):
                        phone_number = '54' + phone_number
                    
                    now = datetime.now()
                    send_time = now + timedelta(minutes=2 + delay_minutes)
                    pywhatkit.sendwhatmsg(phone_number, message, 
                                         send_time.hour, 
                                         send_time.minute)
                    
                    print(f"WhatsApp enviado a {phone_number}")
                    self.log_notification('WhatsApp', row['ACTA'], phone_number, 'Enviado')
            
            # Enviar a ambos números con un pequeño retraso entre ellos
            send_to_number(row['TEL_DOM_LEGAL'])
            send_to_number(row['TEL_DOM_REAL'], delay_minutes=1)
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error al enviar WhatsApp: {error_msg}")
            self.log_notification('WhatsApp', row['ACTA'], 'múltiples números', 'Error', error_msg)

    def send_notifications(self, row):
        if pd.notna(row['MAIL']):
            self.send_email(row)
        if pd.notna(row['TEL_DOM_LEGAL']) or pd.notna(row['TEL_DOM_REAL']):
            self.send_whatsapp(row)

    def check_mdb_files(self):
        df = self.load_mdb_data()
        if df is not None:
            self.check_upcoming_due_dates(df)

def main():
    notification_system = NotificationSystem()
    print("Sistema de notificaciones iniciado. Las notificaciones se registrarán en 'notificaciones.csv'")
    print("El sistema se ejecutará automáticamente todos los días a las 09:00")
    print("Para detener el programa, presione Ctrl+C")
    
    # Programar la verificación diaria
    schedule.every().day.at("09:00").do(notification_system.check_mdb_files)
    
    # Ejecutar verificación inicial al iniciar el programa
    notification_system.check_mdb_files()
    
    # Mantener el programa en ejecución
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()