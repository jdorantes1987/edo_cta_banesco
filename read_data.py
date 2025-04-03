from google.oauth2 import service_account
from googleapiclient.discovery import build
from pandas import DataFrame

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
KEY = "key.json"
# Escribe aquí el ID de tu documento:
SPREADSHEET_ID = "1QeY6G-VkcC-s6B2irJA3M2jVnmxxMvcgCIWiZfc4UCM"

creds = None
creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()


def read_edo_cta(sheet_range):
    # Llamada a la api
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="2025").execute()
    # Extraemos values del resultado en un dataframe de pandas
    values = result.get("values", [])
    return DataFrame(values[1:], columns=values[0])


if __name__ == "__main__":
    print(read_edo_cta("2025"))
