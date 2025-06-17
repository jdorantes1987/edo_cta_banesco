import gspread
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from pandas import merge

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

    def update_edo_cta(self, sheet_name, **kwargs):
        """
        Actualiza los colores y valores de las celdas en una hoja de cálculo de Google Sheets
        según el estado de conciliación de los movimientos bancarios.
        Argumentos:
            sheet_name (str): El nombre de la hoja a actualizar.
            **kwargs: Argumentos adicionales:
            - fecha_d (str, opcional): Fecha de inicio para filtrar movimientos. Por defecto "NULL".
            - fecha_h (str, opcional): Fecha de fin para filtrar movimientos. Por defecto "NULL".
        Funcionalidad:
            - Recupera los movimientos bancarios que necesitan ser actualizados y los combina con
              los movimientos existentes en la hoja.
            - Aplica colores de fondo a las filas según el tipo de movimiento (`tipo_p`):
            - "B1": Conciliado (verde claro).
            - "B2": Otros (gris).
            - "B3": No conciliados (amarillo).
            - "B4": Comisiones IGTF (rosa).
            - Actualiza celdas específicas con valores concatenados o las limpia según el tipo de movimiento.
            - Envía una solicitud de actualización por lotes a la API de Google Sheets para aplicar todos los cambios.
        Notas:
            - La función asume la existencia de una clase `Conciliacion` y una función
              `get_edo_cta_con_identificador` para recuperar y procesar datos.
            - Los objetos `self.sheet_service` y `self.spreadsheet` se utilizan para interactuar con
              la API de Google Sheets.
            - El objeto `self.worksheet` se utiliza para identificar la hoja objetivo para las actualizaciones.
        Excepciones:
            Cualquier excepción generada por la API de Google Sheets durante el proceso de actualización por lotes.
        Imprime:
            Un mensaje de confirmación ("¡colores actualizados!") al completar con éxito.
        """
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")

        oConciliacion = Conciliacion(
            conexion=self.conn,
            sheet_name_edo_cta=sheet_name,
            fecha_d=fecha_d,
            fecha_h=fecha_h,
        )
        mov_actualizar = oConciliacion.get_movimientos_actualizar_edo_cta()[
            [
                "identif_mov_bco",
                "mov_num",
                "cie",
                "fecha_otros_meses",
                "tipo_p",
            ]
        ]
        movimientos_edo_cta = get_edo_cta_con_identificador(sheet_name)

        mov_ident = merge(
            movimientos_edo_cta,
            mov_actualizar,
            how="left",
            left_on="identif_edo_cta",
            right_on="identif_mov_bco",
        )
        # Acumula las celdas que necesitan ser actualizadas
        requests = []

        # definir los colores de fondo
        colors = {
            "conciliado": {"red": 0.84661, "green": 0.99999, "blue": 0.86136},
            "otros": {"red": 0.56497, "green": 0.99999, "blue": 0.99989},
            "rosa_palido": {"red": 0.95649, "green": 0.8349, "blue": 0.90027},
            "comisiones_IGTF": {"red": 0.89007, "green": 0.82291, "blue": 0.79238},
            "no_conciliados": {"red": 1.00005, "green": 0.99999, "blue": 0.48701},
        }

        # recorre los datos de las columnas a y b y establece el color de fondo según la condición
        nro_registros = len(mov_ident)
        for i in range(nro_registros):

            if mov_ident.loc[i, "tipo_p"] == "B1":
                color = colors["conciliado"]
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
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": color,
                                    "textFormat": {
                                        "foregroundColor": {
                                            "red": 0.22116,
                                            "green": 0.31015,
                                            "blue": 0.11965,
                                        }
                                    },
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
                        }
                    }
                )
                cie = mov_ident.loc[i, "cie"]
                nro_mov = mov_ident.loc[i, "mov_num"]
                requests.append(
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": self.worksheet.id,
                                "startRowIndex": i + 1,
                                "endRowIndex": i + 2,
                                "startColumnIndex": 6,
                                "endColumnIndex": 8,
                            },
                            "rows": [
                                {
                                    "values": [
                                        {
                                            "userEnteredValue": {
                                                "stringValue": cie + " -> " + nro_mov
                                            }
                                        },
                                        {"userEnteredValue": {"stringValue": ""}},
                                    ]
                                }
                            ],
                            "fields": "userEnteredValue",
                        }
                    }
                )

            elif mov_ident.loc[i, "tipo_p"] == "B2":
                color = colors["otros"]
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
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": color,
                                    "textFormat": {
                                        "foregroundColor": {
                                            "red": 0.0,
                                            "green": 0.0,
                                            "blue": 0.0,
                                        }
                                    },
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
                        }
                    }
                )
                cie = mov_ident.loc[i, "cie"]
                nro_mov = mov_ident.loc[i, "mov_num"]
                otros_meses = mov_ident.loc[i, "fecha_otros_meses"]
                requests.append(
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": self.worksheet.id,
                                "startRowIndex": i + 1,
                                "endRowIndex": i + 2,
                                "startColumnIndex": 6,
                                "endColumnIndex": 8,
                            },
                            "rows": [
                                {
                                    "values": [
                                        {
                                            "userEnteredValue": {
                                                "stringValue": cie
                                                + " -> "
                                                + nro_mov
                                                + " -> Fecha registro: "
                                                + otros_meses.strftime("%d/%m/%Y")
                                            }
                                        },
                                        {"userEnteredValue": {"stringValue": ""}},
                                    ]
                                }
                            ],
                            "fields": "userEnteredValue",
                        }
                    }
                )

            elif mov_ident.loc[i, "tipo_p"] == "B3":
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
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": color,
                                    "textFormat": {
                                        "foregroundColor": {
                                            "red": 0.46441,
                                            "green": 0.37692,
                                            "blue": 0.08836,
                                        }
                                    },
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
                        }
                    }
                )
                cie = mov_ident.loc[i, "cie"]
                nro_mov = mov_ident.loc[i, "mov_num"]
                requests.append(
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": self.worksheet.id,
                                "startRowIndex": i + 1,
                                "endRowIndex": i + 2,
                                "startColumnIndex": 6,
                                "endColumnIndex": 8,
                            },
                            "rows": [
                                {
                                    "values": [
                                        {"userEnteredValue": {"stringValue": ""}},
                                        {"userEnteredValue": {"stringValue": ""}},
                                    ]
                                }
                            ],
                            "fields": "userEnteredValue",
                        }
                    }
                )

            elif mov_ident.loc[i, "tipo_p"] == "B4":
                color = colors["comisiones_IGTF"]
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
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": color,
                                    "textFormat": {
                                        "foregroundColor": {
                                            "red": 0.31016,
                                            "green": 0.08804,
                                            "blue": 0.02063,
                                        }
                                    },
                                },
                            },
                            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
                        }
                    }
                )
                cie = mov_ident.loc[i, "cie"]
                nro_mov = mov_ident.loc[i, "mov_num"]
                requests.append(
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": self.worksheet.id,
                                "startRowIndex": i + 1,
                                "endRowIndex": i + 2,
                                "startColumnIndex": 6,
                                "endColumnIndex": 8,
                            },
                            "rows": [
                                {
                                    "values": [
                                        {"userEnteredValue": {"stringValue": ""}},
                                        {"userEnteredValue": {"stringValue": ""}},
                                    ]
                                }
                            ],
                            "fields": "userEnteredValue",
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

    from dotenv import load_dotenv

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    load_dotenv(override=True)

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    db = DatabaseConnector(sqlserver_connector)
    fecha_d = "20250101"
    fecha_h = "20250630"
    oEdoCtaUpdate = EdoCtaUpdate(db)
    oEdoCtaUpdate.update_edo_cta("2025", fecha_d=fecha_d, fecha_h=fecha_h)
