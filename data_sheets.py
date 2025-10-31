import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pandas import DataFrame


class ManagerSheets:
    def __init__(self, file_sheet_name, spreadsheet_id, credentials_file):
        self.file_sheet_name = file_sheet_name
        self.spreadsheet_id = spreadsheet_id
        self.credentials_file = credentials_file
        self.spreadsheet = self.get_spreadsheet()

    def get_spreadsheet(self):
        # Autenticación y acceso a Google Sheets
        # oauth2client espera una cadena de scopes; google-auth acepta lista
        self.scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        # Construir credenciales con google-auth (aceptadas por gspread y googleapiclient)
        if isinstance(self.credentials_file, dict):
            self.creds = Credentials.from_service_account_info(
                self.credentials_file, scopes=self.scopes
            )
        else:
            self.creds = Credentials.from_service_account_file(
                self.credentials_file, scopes=self.scopes
            )
        client = gspread.authorize(self.creds)
        return client.open(self.file_sheet_name)

    def get_service(self):
        creds = Credentials.from_service_account_file(
            self.credentials_file, scopes=self.scopes
        )
        return build("sheets", "v4", credentials=creds)

    def get_data_hoja(self, sheet_name) -> DataFrame:
        # Selecciona la hoja de Google Sheets
        worksheet = self.spreadsheet.worksheet(sheet_name)
        # Obtiene todos los valores de la hoja de cálculo
        data = DataFrame(
            worksheet.get_all_values()[1:],  # ignora la primera fila de encabezados
            columns=worksheet.get_all_values()[
                0
            ],  # obtiene la primera fila como encabezados
        )
        return data
