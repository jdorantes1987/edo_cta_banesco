import os
import sys

sys.path.append("..\\profit")
from conn.conexion import DatabaseConnector
from data.mod.compra.cie import CuentasIngresoEgreso
from data.mod.banco.mov_bancarios_oper import MovimientosBacariosOperaciones
from dotenv import load_dotenv
from numpy import where
from pandas import merge, concat

from edo_cta import get_edo_cta_con_identificador
from mov_bco import MovimientosBancarios


class Conciliacion:
    def __init__(self, conexion, sheet_name_edo_cta, fecha_d, fecha_h):
        self.conn = conexion
        self.sheet_name_edo_cta = sheet_name_edo_cta
        self.fecha_d = fecha_d
        self.fecha_h = fecha_h
        self.mov_edo_cta = get_edo_cta_con_identificador(sheet_name_edo_cta)
        self.mov_bancarios = MovimientosBancarios(
            conexion
        ).get_movimientos_bancarios_con_identif(fecha_d=fecha_d, fecha_h=fecha_h)

    def get_movimientos_bancarios_identificados(self):
        conjunto_edo_cta = set(self.mov_edo_cta["identif_edo_cta"])
        movimientos_bancarios = self.mov_bancarios
        conjunto_mov_bco = set(movimientos_bancarios["identif_mov_bco"])
        conjunto_identificados = (
            conjunto_edo_cta & conjunto_mov_bco
        )  # Determina que movimientos se cruzan
        movimientos_bancarios_identificados = movimientos_bancarios[
            movimientos_bancarios["identif_mov_bco"].isin(conjunto_identificados)
        ]
        return movimientos_bancarios_identificados

    def get_movimientos_bancarios_sin_identificar(self, **kwargs):
        mov = kwargs.get("mov", "L")
        movimientos_edo_cta = self.mov_edo_cta
        conjunto_edo_cta = set(movimientos_edo_cta["identif_edo_cta"])
        movimientos_bancarios = self.mov_bancarios
        conjunto_mov_bco = set(movimientos_bancarios["identif_mov_bco"])
        # Si mov es "L" se buscan los movimientos que no se encuentran en los libros
        if mov == "L":
            conjunto_no_identificados = conjunto_mov_bco - conjunto_edo_cta
            # Determina que movimientos se cruzan
            movimientos_por_identificar = movimientos_bancarios[
                movimientos_bancarios["identif_mov_bco"].isin(conjunto_no_identificados)
            ]
        else:
            conjunto_no_identificados = conjunto_edo_cta - conjunto_mov_bco
            movimientos_por_identificar = movimientos_edo_cta[
                movimientos_edo_cta["identif_edo_cta"].isin(conjunto_no_identificados)
            ]
        return movimientos_por_identificar

    def get_movimientos_bancarios_identificados_de_otros_meses(self):
        edo_cta = self.get_movimientos_bancarios_sin_identificar(mov="E").copy()
        libro = self.get_movimientos_bancarios_sin_identificar(mov="L").copy()

        # Estado de cuenta
        # Dividir las columnas en un DataFrame intermedio y seleccionar las últimas dos columnas
        edo_cta[["a", "b"]] = (
            edo_cta["identif_edo_cta"].str.split("|", expand=True).iloc[:, -2:]
        )
        # Combinar las dos últimas columnas para crear una nueva columna
        edo_cta["ref_sin_mes"] = edo_cta["a"] + "|" + edo_cta["b"]

        # eliminar columnas intermedias
        edo_cta.drop(columns=["a", "b"], inplace=True)

        # Libro
        # Dividir las columnas en un DataFrame intermedio y seleccionar las últimas dos columnas
        libro[["a", "b"]] = (
            libro["identif_mov_bco"].str.split("|", expand=True).iloc[:, -2:]
        )
        # Combinar las dos últimas columnas para crear una nueva columna
        libro["ref_sin_mes"] = libro["a"] + "|" + libro["b"]

        # eliminar columnas intermedias
        libro.drop(columns=["a", "b"], inplace=True)

        movimientos_bancarios_identificados = merge(
            edo_cta,
            libro,
            how="inner",
            on="ref_sin_mes",
            suffixes=("_edo_cta", "_libro"),
        )
        return movimientos_bancarios_identificados

    def get_movimientos_actualizar_edo_cta(self):
        columnas_base = [
            "fecha",
            "mov_num",
            "cie",
            "concepto",
            "referencia",
            "monto",
            "identif_mov_bco",
            "tipo_p",
        ]
        mov_identificados = self.get_movimientos_bancarios_identificados().copy()

        # Establece el número de movimiento bancario de acuerdo al tipo de operación (Movimiento de Banco, Cobro o Pago)
        mov_identificados["mov_num"] = where(
            mov_identificados["origen"] != "BAN",
            mov_identificados["cob_pag"],
            mov_identificados["mov_num"],
        )

        # Establece el nombre base que deben tener las columnas
        mov_identificados.rename(
            columns={
                "co_cta_ingr_egr": "cie",
                "descrip": "concepto",
                "doc_num": "referencia",
            },
            inplace=True,
        )

        # Se establece el tipo de partida
        mov_identificados["tipo_p"] = "B1"

        # Se seleccionan las columnas base
        mov_identificados = mov_identificados[columnas_base]

        mov_identificados_otros_meses = (
            self.get_movimientos_bancarios_identificados_de_otros_meses().copy()
        )

        # Resume las columnas del resultado obtenido de los movimientos identificados de otros meses
        mov_identificados_otros_meses = mov_identificados_otros_meses[
            [
                "Fecha",
                "mov_num",
                "co_cta_ingr_egr",
                "descrip",
                "doc_num",
                "monto",
                "origen",
                "cob_pag",
                "correl",
                "identif_edo_cta",
            ]
        ]

        # Establece el nombre base que deben tener las columnas
        mov_identificados_otros_meses.rename(
            columns={
                "Fecha": "fecha",
                "co_cta_ingr_egr": "cie",
                "doc_num": "referencia",
                "descrip": "concepto",
                "doc_num": "referencia",
                "identif_edo_cta": "identif_mov_bco",
            },
            inplace=True,
        )

        # Se establece el tipo de partida
        mov_identificados_otros_meses["tipo_p"] = "B2"

        # Se seleccionan las columnas base
        mov_identificados_otros_meses = mov_identificados_otros_meses[columnas_base]

        mov_sin_identificar = self.get_movimientos_bancarios_sin_identificar(
            mov="E"
        ).copy()

        # Establece el nombre base que deben tener las columnas
        mov_sin_identificar.rename(
            columns={
                "Fecha": "fecha",
                "Estatus": "cie",
                "Referencia": "referencia",
                "Descripción": "concepto",
                "doc_num": "referencia",
                "Monto": "monto",
                "identif_edo_cta": "identif_mov_bco",
            },
            inplace=True,
        )

        # En el caso de las partidas sin identificar no tienen un número de movimiento
        mov_sin_identificar["mov_num"] = ""

        # Se establece el tipo de partida
        mov_sin_identificar["tipo_p"] = "B3"

        # Se seleccionan las columnas base
        mov_sin_identificar = mov_sin_identificar[columnas_base]

        set_mov_identif_otros_meses = set(
            mov_identificados_otros_meses["identif_mov_bco"]
        )

        # se excluye los movimientos identificados de otros meses
        mov_sin_identificar = mov_sin_identificar[
            ~mov_sin_identificar["identif_mov_bco"].isin(set_mov_identif_otros_meses)
        ]

        mov_a_actualizar = concat(
            [
                mov_identificados,
                mov_identificados_otros_meses,
                mov_sin_identificar,
            ],
            axis=0,
        )

        return mov_a_actualizar.sort_values(by=["fecha", "mov_num"], ascending=True)

    def validacion_movimientos_a_insertar(self):
        mov_sin_ident = self.get_movimientos_bancarios_sin_identificar(mov="E")
        cuentas_ing_egr = CuentasIngresoEgreso(
            self.conn
        ).get_cuentas_ingreso_y_egreso()[["co_cta_ingr_egr", "descrip"]]
        data = merge(
            mov_sin_ident,
            cuentas_ing_egr,
            how="left",
            left_on="Estatus",
            right_on="co_cta_ingr_egr",
        )
        data["Comentarios"] = where(
            data["Comentarios"] == "",
            data["Descripción"],
            data["Comentarios"].str[:159],  # Limita la longitud de la cadena
        )
        return data[
            (~data["co_cta_ingr_egr"].isnull())
            & (data["Contabilizar"].str.upper() == "SI")
        ]

    def insertar_movimientos_identificados(self):
        datos = self.validacion_movimientos_a_insertar()
        oMovBancariosOper = MovimientosBacariosOperaciones(self.conn)
        last_id_movbanco = oMovBancariosOper.get_last_id_movbanco("20250331")
        for index, row in datos.iterrows():
            new_id_movbanco = oMovBancariosOper.get_next_id_movbanco(last_id_movbanco)
            oMovBancariosOper.new_movbanco(
                id_m=new_id_movbanco,
                descrip=row["Comentarios"],
                c_ingegr=row["co_cta_ingr_egr"],
                fecha_emision=row["Fecha"].strftime("%Y%m%d"),
                ref_bco=row["Referencia"],
                monto_mov=row["Monto"],
                monto_idb=0.0,
            )
        oMovBancariosOper.confirmar_insercion_movimientos_bancarios()
        return datos


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
    oConciliacion = Conciliacion(
        conexion=oConexion, sheet_name_edo_cta="2025", fecha_d=f_desde, fecha_h=f_hasta
    )
    datos = oConciliacion.insertar_movimientos_identificados()
    print(datos)
