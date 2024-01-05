import win32com.client as win32
import datetime
import pandas as pd

def notifications_error_Connection(url,status_code,formato):
            # Crear una instancia de Outlook
            outlook = win32.Dispatch('Outlook.Application')
            namespace = outlook.GetNamespace("MAPI")

            # Crear un objeto de correo electrónico
            mail = outlook.CreateItem(0)

            # Puedes proporcionar una lista de direcciones de correo electrónico separadas por punto y coma (;)
            recipients = ['jhperez@superfinanciera.gov.co']
            recipients_cc = ['jhperez@superfinanciera.gov.co']
            mail.To = ";".join(recipients)
            mail.cc = ";".join(recipients_cc)

            # Configurar el correo electrónico
            mail.Subject ='Conexion Fallida con la Pagina de Datos Abiertos'
            
            message_html = f"""
            <html>
            <body>
                <p>Buen día,</p>
                <p>El proceso de conexión para la URL <em>{url}</em> de datos abiertos la cual corresponde al formato <em>{formato}</em> presentó fallas para el día {datetime.datetime.now().date()}.</p>
                <br>
                <p>Atentamente</p>
                <p style="margin: 0;"><strong>Celula Analitica de Mercado de Capitales</strong></p>
                <p style="margin: 0;">Superintendencia Financiera de Colombia - 100 Años</p>
                <p style="margin: 0;">Conmutador: +57 6015940200 exts.4556</p>
                <p style="margin: 0;">Calle 7 No. 4 - 49 Bogotá D.C., Colombia www.superfinanciera.gov.co</p>
            </body>
            </html>

            """
            mail.BodyFormat = 2  # 2 significa HTML
            mail.HTMLBody = message_html
            mail.Send()

def leer_diferencias_desde_csv(nombre_archivo):
    # Leer las diferencias desde el archivo CSV
    diferencias_df = pd.read_csv(nombre_archivo, sep='|')
    diferencias = diferencias_df.to_dict(orient='records')
    return diferencias

def notificacion_final(format, difference,start_date,end_date, file, log):
        # Crear una instancia de Outlook
        outlook = win32.Dispatch('Outlook.Application')
        namespace = outlook.GetNamespace("MAPI")

        # Crear un objeto de correo electrónico
        mail = outlook.CreateItem(0)

        # Puedes proporcionar una lista de direcciones de correo electrónico separadas por punto y coma (;)
        recipients = ['jhperez@superfinanciera.gov.co']
        recipients_cc = ['jhperez@superfinanciera.gov.co']
        mail.To = ";".join(recipients)
        mail.cc = ";".join(recipients_cc)

        # Configurar el correo electrónico
        mail.Subject =f'Validacion Informacion F{format} Datos Abiertos'

        if difference ==0:            
            message_html = f"""
            <html>
            <body>
                <p>Buen día,</p>
                <p>El proceso de validacion ejecutada el dia  {datetime.datetime.now().date()} entre Datos Abiertos y Teradata relacionados al Formato <em>{format}</em> para el corte comprendido entre <em>{start_date}<em> y <em>{end_date}<em> finalizaron exitosamente.</p>            
                <br>
                <p>Atentamente</p>
                <p style="margin: 0;"><strong>Celula Analitica de Mercado de Capitales</strong></p>
                <p style="margin: 0;">Superintendencia Financiera de Colombia - 100 Años</p>
                <p style="margin: 0;">Conmutador: +57 6015940200 exts.4556</p>
                <p style="margin: 0;">Calle 7 No. 4 - 49 Bogotá D.C., Colombia www.superfinanciera.gov.co</p>
            </body>
            </html>

            """
            
        else:
            diferencias_leidas = leer_diferencias_desde_csv(file)
            message_html = f"""
                    <html>
                    <body>
                        <p>Buen día,</p>
                        <p>El proceso de validación ejecutada el día {datetime.datetime.now().date()} entre Datos Abiertos y Teradata relacionados al Formato <em>{format}</em> para el corte comprendido entre <em>{start_date}</em> y <em>{end_date}</em> finalizó exitosamente presentando las siguientes diferencias:</p>            
                        <ul>
                    """

                    # Agregar cada elemento como un ítem de lista en el correo HTML
            for diferencia in diferencias_leidas:
                message_html += f"<li>{diferencia['Observacion']}: {diferencia['Cantidad']}</li>"
                #agregar adjunto
                mail.Attachments.Add(diferencia['Ruta'])

            message_html += """
                </ul>
                <p>Atentamente</p>
                <p style="margin: 0;"><strong>Celula Analitica de Mercado de Capitales</strong></p>
                <p style="margin: 0;">Superintendencia Financiera de Colombia - 100 Años</p>
                <p style="margin: 0;">Conmutador: +57 6015940200 exts.4556</p>
                <p style="margin: 0;">Calle 7 No. 4 - 49 Bogotá D.C., Colombia www.superfinanciera.gov.co</p>
            </body>
            </html>
            """
        mail.Attachments.Add(log)
        mail.BodyFormat = 2  # 2 significa HTML
        mail.HTMLBody = message_html
        mail.Send()

