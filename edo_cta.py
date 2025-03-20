import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pandas import DataFrame, to_datetime
from functions import get_identificador_unicos

# Autenticación y acceso a Google Sheets
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("key.json", scope)


def get_edo_cta_con_identificador(sheet_name: str) -> DataFrame:
    client = gspread.authorize(creds)
    spreadsheet = client.open("edo_cta_banesco")
    lista_columnas = ["Fecha", "Referencia", "Descripción", "Monto", "identificador"]
    worksheet = spreadsheet.worksheet(sheet_name)

    # Obtiene todos los valores de la hoja de cálculo
    df_edo_cta = DataFrame(
        worksheet.get_all_values()[1:],
        columns=worksheet.get_all_values()[0],  # Selecciona las filas y columnas
    )
    df_edo_cta = df_edo_cta[
        df_edo_cta["Fecha"].notnull()
    ]  # Elimina las filas que no tienen fecha asociada
    # Convertir una columna a float
    df_edo_cta["Monto"] = df_edo_cta["Monto"].astype("str")
    df_edo_cta["Monto"] = df_edo_cta["Monto"].str.replace(
        ".",
        "",
    )  # Eliminar separadores de miles
    df_edo_cta["Monto"] = df_edo_cta["Monto"].str.replace(
        ",",
        ".",
    )  # Reemplazar coma decimal por punto
    df_edo_cta["Monto"] = df_edo_cta["Monto"].astype(float)
    df_edo_cta["Fecha"] = to_datetime(df_edo_cta["Fecha"], format="%d/%m/%Y")
    df_edo_cta["unicos"] = (
        df_edo_cta["Fecha"].dt.month.astype("str")
        + "|"
        + df_edo_cta["Referencia"]
        + "|"
        + df_edo_cta["Monto"].astype("str")
    )
    df_edo_cta = get_identificador_unicos(df_edo_cta, "unicos")[lista_columnas]
    df_edo_cta.rename(columns={"identificador": "identif_edo_cta"}, inplace=True)
    return df_edo_cta


if __name__ == "__main__":
    print(get_edo_cta_con_identificador("2025"))
