import logging
import logging.config
import time
from datetime import datetime

import gspread
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials

from conciliacion import Conciliacion
from edo_cta_update import EdoCtaUpdate

# Autenticación y acceso a Google Sheets
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Cargar credenciales
creds = ServiceAccountCredentials.from_json_keyfile_name("key.json", scope)
client = gspread.authorize(creds)


class GoogleSheetMonitor:
    def __init__(self, conexion):
        self.conexion = conexion
        self.drive_service = build("drive", "v3", credentials=creds)

    def load_page_token(self):
        """Carga el último token de página guardado."""
        try:
            with open("page_token.txt", "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    def save_page_token(self, token):
        """Guarda el token de página actual."""
        try:
            with open("page_token.txt", "w") as f:
                f.write(token)
        except Exception as e:
            logging.error(f"Error al guardar el token de página: {e}", exc_info=True)

    def get_file_details(self, file_id):
        """
        Obtiene los detalles del archivo, incluyendo el último usuario que lo modificó.
        """
        try:
            file = (
                self.drive_service.files()
                .get(fileId=file_id, fields="name,lastModifyingUser")
                .execute()
            )
            return file
        except Exception as e:
            logging.error(f"Error al obtener detalles del archivo: {e}", exc_info=True)
            return None

    def monitor_sheet_changes(self, sheet_id, **kwargs):
        """
        Monitorea cambios en una hoja de cálculo específica.
        Args:
            sheet_id: El ID de la hoja de cálculo a monitorear.
        """

        oEdoCtaUpdate = EdoCtaUpdate(self.conexion)
        cambios = False
        excluidos = [
            "estados-de-cuenta-bantel@bantel-sheets.iam.gserviceaccount.com",
        ]
        try:
            # Obtener el último token guardado o solicitar uno nuevo
            page_token = self.load_page_token()
            if not page_token:
                response = self.drive_service.changes().getStartPageToken().execute()
                page_token = response.get("startPageToken")
                self.save_page_token(page_token)
            while True:
                try:
                    while True:
                        response = (
                            self.drive_service.changes()
                            .list(
                                pageToken=page_token,
                                fields="nextPageToken,newStartPageToken,changes(fileId,file)",
                            )
                            .execute()
                        )

                        for change in response.get("changes", []):
                            if change.get("fileId") == sheet_id:
                                logging.info("¡Se detectaron cambios en la hoja!")
                                # Obtener detalles del archivo
                                file_details = self.get_file_details(sheet_id)
                                if file_details:
                                    file_name = file_details.get("name", "Desconocido")
                                    last_user = file_details.get(
                                        "lastModifyingUser", {}
                                    ).get("displayName", "Desconocido")
                                    logging.info(f"Archivo modificado: {file_name}")
                                    logging.info(f"Modificado por: {last_user}")
                                    # Cambia a True si el usuario no está en la lista de excluidos
                                    cambios = last_user not in excluidos
                                    if cambios is True:
                                        tConciliacion = Conciliacion(
                                            conexion=self.conexion,
                                            sheet_name_edo_cta="2025",
                                            fecha_d=kwargs.get("fecha_d", "NULL"),
                                            fecha_h=kwargs.get("fecha_h", "NULL"),
                                        )
                                        print(
                                            f"insertar_movimientos_identificados: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
                                        )
                                        tConciliacion.insertar_movimientos_identificados(
                                            ultima_fecha=kwargs.get("fecha_h", "NULL"),
                                        )
                                        # Si se detectaron cambios, actualizar la hoja de cálculo
                                        logging.info(
                                            f"actualizar_colores_edo_cta: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                                        )
                                        oEdoCtaUpdate.update_edo_cta("2025", **kwargs)
                                        cambios = False

                        # Si hay más páginas de cambios, sigue recorriéndolas
                        if "nextPageToken" in response:
                            page_token = response["nextPageToken"]
                        else:
                            # Solo guarda el newStartPageToken cuando ya no hay más páginas
                            if "newStartPageToken" in response:
                                self.save_page_token(response["newStartPageToken"])
                                page_token = response["newStartPageToken"]
                            break

                    # Esperar 60 segundos antes de la próxima verificación
                    time.sleep(60)
                except Exception as e:
                    logging.error(f"Error al procesar cambios: {e}", exc_info=True)
                    time.sleep(10)  # Espera antes de reintentar

        except HttpError as error:
            logging.error(f"Ocurrió un error: {error}", exc_info=True)
        except Exception as e:
            logging.error(f"Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":

    import os

    from conn.conexion import DatabaseConnector
    from dotenv import load_dotenv

    load_dotenv()
    logging.config.fileConfig("logging.ini")
    # Para SQL Server
    datos_conexion = dict(
        host=os.getenv("HOST_PRODUCCION_PROFIT"),
        base_de_datos=os.getenv("DB_NAME_DERECHA_PROFIT"),
    )
    oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
    fecha_d = "20250101"
    fecha_h = "20250630"
    # Crear instancia de GoogleSheetMonitor
    oMonitor = GoogleSheetMonitor(oConexion)
    try:
        sheet_id = "1QeY6G-VkcC-s6B2irJA3M2jVnmxxMvcgCIWiZfc4UCM"
        oMonitor.monitor_sheet_changes(sheet_id, fecha_d=fecha_d, fecha_h=fecha_h)
    except Exception as e:
        print(f"Error al iniciar el monitoreo: {e}")
