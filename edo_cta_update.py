import gspread
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from pandas import merge
from numpy import nan

from conciliacion import Conciliacion
from edo_cta import get_edo_cta_con_identificador


class EdoCtaUpdate:

    def __init__(self, conexion):
        self.conn = conexion
        # Autenticación y acceso a Google Sheets
        self.scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(
            "key.json", self.scope
        )
        client = gspread.authorize(self.creds)
        self.spreadsheet = client.open("edo_cta_banesco")

        # Selecciona la hoja de Google Sheets
        self.worksheet = self.spreadsheet.worksheet("2025")

        # Construir el servicio de la API de Google Sheets
        self.sheet_service = build("sheets", "v4", credentials=self.creds)

    def update_edo_cta_movimientos_identificados(self, sheet_name, **kwargs):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")

        oConciliacion = Conciliacion(
            conexion=self.conn,
            sheet_name_edo_cta=sheet_name,
            fecha_d=fecha_d,
            fecha_h=fecha_h,
        )
        movimientos_bancarios = oConciliacion.get_movimientos_bancarios_identificados(
            fecha_d=fecha_d, fecha_h=fecha_h
        )

        movimientos_edo_cta = get_edo_cta_con_identificador(sheet_name)

        movimientos_identificados = merge(
            movimientos_edo_cta,
            movimientos_bancarios,
            how="left",
            left_on="identif_edo_cta",
            right_on="identif_mov_bco",
        )

        # Acumula las celdas que necesitan ser actualizadas
        requests = []

        # definir los colores de fondo
        colors = {
            "greater": {"red": 0.67, "green": 0.89, "blue": 0.75},
            "equal": {"red": 0.29, "green": 0.78, "blue": 0.52},
            "zero": {"red": 1, "green": 0.61, "blue": 0.53},
            "default": {"red": 1, "green": 1, "blue": 1},
        }

        # recorre los datos de las columnas a y b y establece el color de fondo según la condición
        nro_registros = len(movimientos_identificados)
        for i in range(nro_registros):
            if movimientos_identificados.loc[i, "identif_mov_bco"] is not nan:
                color = colors["greater"]
                # agregar la solicitud de actualización de color
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": self.worksheet.id,
                                "startRowIndex": i + 1,
                                "endRowIndex": i + 2,
                                "startColumnIndex": 0,
                                "endColumnIndex": 5,
                            },
                            "cell": {"userEnteredFormat": {"backgroundColor": color}},
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    }
                )

        # si hay celdas que necesitan ser actualizadas, hacer una sola llamada a la api
        if requests:
            body = {"requests": requests}
            self.sheet_service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet.id, body=body
            ).execute()

        print("¡colores actualizados!")

    def update_edo_cta_movimientos_identificados_otros_meses(
        self, sheet_name, **kwargs
    ):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")

        oConciliacion = Conciliacion(
            conexion=self.conn,
            sheet_name_edo_cta=sheet_name,
            fecha_d=fecha_d,
            fecha_h=fecha_h,
        )
        movimientos_bancarios_otros_meses = (
            oConciliacion.get_movimientos_bancarios_identificados_de_otros_meses()[
                ["identif_edo_cta", "identif_mov_bco"]
            ]
        )

        movimientos_edo_cta = get_edo_cta_con_identificador(sheet_name)

        movimientos_identificados = merge(
            movimientos_edo_cta,
            movimientos_bancarios_otros_meses,
            how="left",
            left_on="identif_edo_cta",
            right_on="identif_edo_cta",
        )

        # Acumula las celdas que necesitan ser actualizadas
        requests = []

        # definir los colores de fondo
        colors = {
            "greater": {"red": 0.55327, "green": 0.61775, "blue": 0.76934},
            "equal": {"red": 0.29, "green": 0.78, "blue": 0.52},
            "zero": {"red": 1, "green": 0.61, "blue": 0.53},
            "default": {"red": 1, "green": 1, "blue": 1},
        }

        # recorre los datos de las columnas a y b y establece el color de fondo según la condición
        nro_registros = len(movimientos_identificados)
        for i in range(nro_registros):
            if movimientos_identificados.loc[i, "identif_mov_bco"] is not nan:
                color = colors["greater"]
                # agregar la solicitud de actualización de color
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": self.worksheet.id,
                                "startRowIndex": i + 1,
                                "endRowIndex": i + 2,
                                "startColumnIndex": 0,
                                "endColumnIndex": 5,
                            },
                            "cell": {"userEnteredFormat": {"backgroundColor": color}},
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    }
                )

        # si hay celdas que necesitan ser actualizadas, hacer una sola llamada a la api
        if requests:
            body = {"requests": requests}
            self.sheet_service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet.id, body=body
            ).execute()

        print("¡colores actualizados!")

    def update_edo_cta_movimientos_sin_identificar(self, sheet_name, **kwargs):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")

        oConciliacion = Conciliacion(
            conexion=self.conn,
            sheet_name_edo_cta=sheet_name,
            fecha_d=fecha_d,
            fecha_h=fecha_h,
        )
        movimientos_sin_identificar_edo_cta = (
            oConciliacion.get_movimientos_bancarios_sin_identificar(mov="E")[
                ["identif_edo_cta", "Referencia"]
            ]
        )

        movimientos_edo_cta = get_edo_cta_con_identificador(sheet_name)

        movimientos_identificados_no_identificados = merge(
            movimientos_edo_cta,
            movimientos_sin_identificar_edo_cta,
            how="left",
            left_on="identif_edo_cta",
            right_on="identif_edo_cta",
            suffixes=("_orig", "_aux"),
        )

        # Acumula las celdas que necesitan ser actualizadas
        requests = []

        # definir los colores de fondo
        colors = {
            "no_conciliados": {"red": 0.97169, "green": 0.97163, "blue": 0.47108},
        }

        # recorre los datos de las columnas a y b y establece el color de fondo según la condición
        nro_registros = len(movimientos_identificados_no_identificados)
        for i in range(nro_registros):
            if (
                movimientos_identificados_no_identificados.loc[i, "Referencia_aux"]
                is not nan
            ):
                color = colors["no_conciliados"]
                # agregar la solicitud de actualización de color
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": self.worksheet.id,
                                "startRowIndex": i + 1,
                                "endRowIndex": i + 2,
                                "startColumnIndex": 0,
                                "endColumnIndex": 5,
                            },
                            "cell": {"userEnteredFormat": {"backgroundColor": color}},
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    }
                )

        # si hay celdas que necesitan ser actualizadas, hacer una sola llamada a la api
        if requests:
            body = {"requests": requests}
            self.sheet_service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet.id, body=body
            ).execute()

        print("¡colores actualizados!")


if __name__ == "__main__":
    import os

    from conn.conexion import DatabaseConnector
    from dotenv import load_dotenv

    load_dotenv()
    # Para SQL Server
    datos_conexion = dict(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        base_de_datos=os.environ["DB_NAME_DERECHA_PROFIT"],
    )
    oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
    fecha_d = "20250101"
    fecha_h = "20250331"
    oEdoCtaUpdate = EdoCtaUpdate(oConexion)
    oEdoCtaUpdate.update_edo_cta_movimientos_identificados(
        "2025", fecha_d=fecha_d, fecha_h=fecha_h
    )
