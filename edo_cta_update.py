from pandas import merge

from conciliacion import Conciliacion
from edo_cta import get_edo_cta_con_identificador


class EdoCtaUpdate:
    def __init__(self, conexion, manager_sheets):
        self.conn = conexion
        self.manager_sheets = manager_sheets
        self.service = self.manager_sheets.get_service()

    def update_edo_cta(self, sheet_name, **kwargs):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")
        self.spreadsheet = self.manager_sheets.get_spreadsheet(sheet_name=sheet_name)
        sheetId = (
            self.service.spreadsheets()
            .get(spreadsheetId=self.spreadsheet.id)
            .execute()
            .get("sheets", [])[0]
            .get("properties", {})
            .get("sheetId")
        )
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
                                "sheetId": sheetId,
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
                                "sheetId": sheetId,
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
                                "sheetId": sheetId,
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
                                "sheetId": sheetId,
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
                                "sheetId": sheetId,
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
                                "sheetId": sheetId,
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
                                "sheetId": sheetId,
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
                                "sheetId": sheetId,
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
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet.id, body=body
            ).execute()

        print("¡colores actualizados!")


if __name__ == "__main__":
    import os
    import sys

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector
    from dotenv import load_dotenv

    from data_sheets import ManagerSheets

    sys.path.append("../conexiones")

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.getenv("HOST_PRODUCCION_PROFIT"),
        database=os.getenv("DB_NAME_DERECHA_PROFIT"),
        user=os.getenv("DB_USER_PROFIT"),
        password=os.getenv("DB_PASSWORD_PROFIT"),
    )
    db = DatabaseConnector(sqlserver_connector)
    oManager = ManagerSheets(
        file_sheet_name=os.getenv("FILE_EDO_CTA_NAME"),
        spreadsheet_id=os.getenv("FILE_EDO_CTA_ID"),
        credentials_file=os.getenv("EDO_CTA_CREDENTIALS"),
    )
    fecha_d = "20250101"
    fecha_h = "20250731"
    oEdoCtaUpdate = EdoCtaUpdate(db, oManager)
    oEdoCtaUpdate.update_edo_cta("2025", fecha_d=fecha_d, fecha_h=fecha_h)
