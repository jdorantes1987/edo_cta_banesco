import os
import sys

from dotenv import load_dotenv
from pandas import DataFrame, to_datetime

from data_sheets import ManagerSheets
from functions import get_identificador_unicos


def get_edo_cta_con_identificador(sheet_name: str) -> DataFrame:
    sys.path.append("../conexiones")
    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    oManager = ManagerSheets(
        file_sheet_name=os.getenv("FILE_EDO_CTA_NAME"),
        spreadsheet_id=os.getenv("FILE_EDO_CTA_ID"),
        credentials_file=os.getenv("EDO_CTA_CREDENTIALS"),
    )
    df_edo_cta = oManager.get_data_hoja(sheet_name)
    lista_columnas = [
        "Fecha",
        "Referencia",
        "Descripción",
        "Monto",
        "Comentarios",
        "Estatus",
        "Contabilizar",
        "identificador",
    ]

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
