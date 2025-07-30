import sys

from numpy import where

sys.path.append("..\\profit")

from data.sql_read import ReadSql
from pandas import to_datetime

from functions import get_identificador_unicos


class MovimientosBancarios:
    def __init__(self, conexion):
        self.conn = conexion
        self.oRead_sql = ReadSql(conexion)

    def get_movimientos_bancarios(self, **kwargs):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")

        columns_select = [
            "fecha",
            "mov_num",
            "co_cta_ingr_egr",
            "descrip",
            "doc_num",
            "monto",
            "origen",
            "cob_pag",
        ]

        sql = f"""
                EXEC [dbo].[RepMoviBancoXFec]
                @cCo_CodCuenta_d = N'0134',
                @cCo_CodCuenta_h = N'0134',
                @sFecha_d = '{fecha_d}',
                @sFecha_h = '{fecha_h}'
              """.replace(
            "'NULL'", "NULL"
        )

        datos = self.oRead_sql.get_data(sql)
        # Se debe cambiar el nombre de la columna, a través de cada índice, debido a que la consulta no asigna un nombre único a cada columna
        datos.columns.values[8] = "monto_d_encab"
        datos.columns.values[9] = "monto_h_encab"
        columnas_a_limpiar_espacios = ["mov_num", "doc_num", "co_cta_ingr_egr"]

        # Aplicar strip() a varias columnas
        datos[columnas_a_limpiar_espacios] = datos[columnas_a_limpiar_espacios].apply(
            lambda col: col.str.strip()
        )

        # Crear una nueva columna con el monto y signo correspondiente
        datos["monto"] = where(
            datos["monto_d"] > 0, -datos["monto_d"], datos["monto_h"]
        )
        return datos[columns_select]

    def get_movimientos_bancarios_con_identif(self, **kwargs):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")
        datos = self.get_movimientos_bancarios(fecha_d=fecha_d, fecha_h=fecha_h).copy()
        datos["fecha"] = to_datetime(datos["fecha"])
        datos["unicos"] = (
            datos["fecha"].dt.month.astype("str")
            + "|"
            + datos["doc_num"]
            + "|"
            + datos["monto"].astype("str")
        )
        datos = get_identificador_unicos(datos, "unicos").copy()
        datos.drop(columns=["unicos"], inplace=True)
        datos.rename(columns={"identificador": "identif_mov_bco"}, inplace=True)
        return datos


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    sys.path.append("..\\profit")

    env_path = os.path.join("..\\profit", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    db = DatabaseConnector(sqlserver_connector)
    fecha_d = "20250101"
    fecha_h = "20250331"
    mov_bco = MovimientosBancarios(db).get_movimientos_bancarios_con_identif(
        fecha_d=fecha_d, fecha_h=fecha_h
    )
    print(mov_bco)
