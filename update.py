import gspread
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from pandas import DataFrame
from functions import get_identificador_unicos

# Autenticación y acceso a Google Sheets
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("key.json", scope)
client = gspread.authorize(creds)
spreadsheet = client.open("edo_cta_banesco")


def update_edo_cta():
    # Selecciona la hoja de Google Sheets
    worksheet = spreadsheet.worksheet("2025")

    # Construir el servicio de la API de Google Sheets
    sheet_service = build("sheets", "v4", credentials=creds)

    # Quitar los filtros de la hoja
    clear_filter_request = {"clearBasicFilter": {"sheetId": worksheet.id}}
    sheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet.id, body={"requests": [clear_filter_request]}
    ).execute()

    # Obtiene todos los valores de la hoja de cálculo
    all_values = DataFrame(
        worksheet.get_all_values()[1:],
        columns=worksheet.get_all_values()[0],  # Selecciona las filas y columnas
    )

    # Establece un identificador único para cada transacción
    all_values = get_identificador_unicos(df=all_values, name_field="Monto")
    all_values.drop(columns=["correl"], axis=1, inplace=True)

    # Acumula las celdas que necesitan ser actualizadas
    requests = []

    # Definir los colores de fondo
    colors = {
        "greater": {"red": 0.67, "green": 0.89, "blue": 0.75},
        "equal": {"red": 0.29, "green": 0.78, "blue": 0.52},
        "zero": {"red": 1, "green": 0.61, "blue": 0.53},
        "default": {"red": 1, "green": 1, "blue": 1},
    }

    # Recorre los datos de las columnas A y B y establece el color de fondo según la condición
    for i in range(len(all_values)):
        if all_values.loc[i, "Contabilizar"] == "NO":
            color = colors["equal"]
            # Agregar la solicitud de actualización de color
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": worksheet.id,
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

    # Si hay celdas que necesitan ser actualizadas, hacer una sola llamada a la API
    if requests:
        body = {"requests": requests}
        sheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet.id, body=body
        ).execute()

    print("¡Colores actualizados!")


if __name__ == "__main__":
    update_edo_cta()
