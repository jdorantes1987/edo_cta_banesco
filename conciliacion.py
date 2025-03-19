import os
import sys

sys.path.append("..\\profit")
from conn.conexion import DatabaseConnector
from dotenv import load_dotenv

from mov_bco import MovimientosBancarios
from edo_cta import get_edo_cta_con_identificador


class Conciliacion:
    def __init__(self, conexion):
        self.conn = conexion

    def movimientos_bancarios_identificados(self, **kwargs):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")
        conjunto_edo_cta = set(get_edo_cta_con_identificador("2025")["identif_edo_cta"])
        movimientos_bancarios = MovimientosBancarios(
            self.conn
        ).get_movimientos_bancarios_con_identif(fecha_d=fecha_d, fecha_h=fecha_h)
        conjunto_mov_bco = set(movimientos_bancarios["identif_mov_bco"])
        conjunto_identificados = conjunto_edo_cta & conjunto_mov_bco
        movimientos_bancarios = movimientos_bancarios[
            movimientos_bancarios["identif_mov_bco"].isin(conjunto_identificados)
        ]
        return movimientos_bancarios


if __name__ == "__main__":
    f_desde = "20250101"
    f_hasta = "20250331"
    load_dotenv()
    # Para SQL Server
    datos_conexion = dict(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        base_de_datos=os.environ["DB_NAME_DERECHA_PROFIT"],
    )
    oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
    oConciliacion = Conciliacion(oConexion)
    datos = oConciliacion.movimientos_bancarios_identificados(
        fecha_d=f_desde, fecha_h=f_hasta
    )
    print(datos)
