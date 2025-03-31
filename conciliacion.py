import os
import sys

sys.path.append("..\\profit")
from conn.conexion import DatabaseConnector
from data.mod.compra.cie import CuentasIngresoEgreso
from data.mod.banco.mov_bancarios_oper import MovimientosBacariosOperaciones
from dotenv import load_dotenv
from numpy import where
from pandas import merge, concat, DataFrame

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

        comisiones_igtf = self.get_comisiones_e_igtf_sin_registrar()

        # Establece el nombre base que deben tener las columnas para las comisiones de IGTF
        comisiones_igtf.rename(
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

        # Crea una columna vacía para el número de movimiento bancario
        comisiones_igtf["mov_num"] = ""

        # Se establece el tipo de partida
        comisiones_igtf["tipo_p"] = "B4"

        # Se seleccionan las columnas base
        comisiones_igtf = comisiones_igtf[columnas_base]

        set_comisiones_igtf = set(comisiones_igtf["identif_mov_bco"])

        # Establece el nombre base que deben tener las columnas para los movimientos sin identificar
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

        # Crea una columna vacía para el número de movimiento bancario
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

        # se excluye las comisiones e IGTF
        mov_sin_identificar = mov_sin_identificar[
            ~mov_sin_identificar["identif_mov_bco"].isin(set_comisiones_igtf)
        ]

        mov_a_actualizar = concat(
            [
                mov_identificados,
                mov_identificados_otros_meses,
                mov_sin_identificar,
                comisiones_igtf,
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

    def insertar_movimientos_identificados(self, ultima_fecha):
        datos = self.validacion_movimientos_a_insertar()
        oMovBancariosOper = MovimientosBacariosOperaciones(self.conn)
        last_id_movbanco = oMovBancariosOper.get_last_id_movbanco(ultima_fecha)
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
            print(
                new_id_movbanco
                + " --> "
                + row["Comentarios"]
                + " --> "
                + str(row["Monto"])
            )
        oMovBancariosOper.confirmar_insercion_movimientos_bancarios()
        return datos

    def insertar_movimientos_comisiones_igtf(self, fecha_d, fecha_h):
        datos = oConciliacion.get_comisiones_e_igtf_sin_registrar()
        # Filtra entre fechas
        datos = datos[(datos["Fecha"] >= fecha_d) & (datos["Fecha"] <= fecha_h)]
        oMovBancariosOper = MovimientosBacariosOperaciones(self.conn)
        last_id_movbanco = oMovBancariosOper.get_last_id_movbanco(fecha_h)
        for index, row in datos.iterrows():
            new_id_movbanco = oMovBancariosOper.get_next_id_movbanco(last_id_movbanco)
            oMovBancariosOper.new_movbanco(
                id_m=new_id_movbanco,
                descrip=row["Descripción"][:159],
                c_ingegr=row["co_cta_ingr_egr"],
                fecha_emision=row["Fecha"].strftime("%Y%m%d"),
                ref_bco=row["Referencia"],
                monto_mov=row["Monto"],
                monto_idb=0.0,
            )
            print(
                new_id_movbanco
                + " --> "
                + row["Descripción"]
                + " --> "
                + str(row["Monto"])
            )
        oMovBancariosOper.confirmar_insercion_movimientos_bancarios()
        return datos

    def get_comisiones_e_igtf_sin_registrar(self):
        comisiones_igtf = self.get_mov_igtf_comisiones(
            fecha_d=self.fecha_d, fecha_h=self.fecha_h
        )
        set_mov_sin_identificar = set(
            self.get_movimientos_bancarios_sin_identificar(mov="E")["identif_edo_cta"]
        )
        # Filtra solo aquellas comisiones que están pendiente por registrar
        comisiones_igtf = comisiones_igtf[
            comisiones_igtf["identif_edo_cta"].isin(set_mov_sin_identificar)
        ]
        return comisiones_igtf

    def get_mov_igtf_comisiones(self, fecha_d, fecha_h, **kwargs):
        data_edo_cta = self.mov_edo_cta
        # filtra los valores no nulos y entre fechas
        f_edo_cta_sin_nulos = data_edo_cta[
            (data_edo_cta["Fecha"].notnull())
            & (data_edo_cta["Fecha"] != "")
            & (data_edo_cta["Fecha"] >= fecha_d)
            & (data_edo_cta["Fecha"] <= fecha_h)
        ]
        edo_cta_ref_counts = DataFrame(f_edo_cta_sin_nulos["Referencia"].value_counts())
        # Cambia el nombre de la columna
        edo_cta_ref_counts.rename(columns={"count": "repeticiones"}, inplace=True)
        # Une los DataFrame a traves de la clausula LeftJoint
        f_edo_cta_ref_repet = merge(
            f_edo_cta_sin_nulos,
            edo_cta_ref_counts,
            left_on="Referencia",
            right_on="Referencia",
        )
        # -----------------------------OBTENER IGTF DE PAGOS CON 4 REPETICIONES EN REFERENCIAS--------------------------------->
        filtrar_solo_igtf_sort = f_edo_cta_ref_repet.sort_values(
            by=["Fecha", "Referencia", "Monto"], ascending=[True, True, True]
        )
        igtf_huerf = filtrar_solo_igtf_sort[
            filtrar_solo_igtf_sort["repeticiones"] >= 2
        ].copy()  # Se hace una copia del dataframe para que no me arroje advertencia
        igtf_huerf["Monto_Ant"] = igtf_huerf["Monto"].shift(
            1
        )  # Obtiene el elemento anterior en relacion al elemento actual
        # Obtiene el porcentaje que representa el monto del elemento actual respecto al anterior
        igtf_huerf["porcentaje"] = igtf_huerf.apply(
            lambda x: round(x["Monto"] / x["Monto_Ant"], ndigits=2), axis=1
        )
        solo_igtf_sort2 = igtf_huerf[
            (igtf_huerf["porcentaje"] <= 0.02)
        ]  # | (edo_cta_igtf_huerf['porcentaje'] == 0.01)
        # Recuerda que al trabajar con números negativos los valores máximos son los que estan más cerca del cero
        # Obtiene los valores máximos de cada grupo de referencias
        igtf_group = solo_igtf_sort2.loc[
            solo_igtf_sort2.groupby("Referencia", sort=False).Monto.idxmax()
        ].reset_index(
            drop=True
        )  # sort=False para que no ordene los grupos
        igtf_pagos = igtf_group[
            (igtf_group["repeticiones"] == 4) & (igtf_group["Monto"] < 0)
        ]
        # print("IGTF de pagos con 4 repeticiones en las referencias bancarias \n", igtf_pagos.to_string())
        # -----------------------------OBTENER IGTF DE COBROS CON 3 REPETICIONES EN REFERENCIAS--------------------->
        igtf_cob_rep_3 = solo_igtf_sort2[
            (solo_igtf_sort2["repeticiones"] == 3) & (solo_igtf_sort2["Monto"] > 0)
        ]
        # Intersección de dos columnas en dos marcos de datos
        igtf_cob_rep_3_inters = solo_igtf_sort2[
            solo_igtf_sort2["Referencia"].isin(igtf_cob_rep_3["Referencia"])
        ].copy()
        filtrar_igtf_rep_3 = igtf_cob_rep_3_inters[
            (igtf_cob_rep_3_inters["porcentaje"] == 0.02)
            & (igtf_cob_rep_3_inters["repeticiones"] == 3)
        ]
        # print("IGTF de cobros con 3 repeticiones en las referencias bancarias\n",
        #       filtrar_igtf_rep_3.reset_index(drop=True).to_string())
        # ----------------------------------------OBTENER COMISIONES DE PAGOS-------------------------------------------------->

        edo_cta_sin_igtf = igtf_huerf[igtf_huerf["porcentaje"] != 0.02].copy()
        edo_cta_sin_igtf["Monto_Ant"] = edo_cta_sin_igtf["Monto"].shift(1)
        edo_cta_sin_igtf["porcentaje"] = edo_cta_sin_igtf.apply(
            lambda x: round(x["Monto"] / x["Monto_Ant"], ndigits=3), axis=1
        )
        com_de_pagos = edo_cta_sin_igtf[
            (edo_cta_sin_igtf["porcentaje"] == 0.003)
            | (edo_cta_sin_igtf["porcentaje"] == 0.004)
        ]
        # print("Comisiones de pagos\n", com_de_pagos.to_string())
        # ----------------------------------------OBTENER COMISIONES DE COBROS------------------------------------------------->
        edo_cta_sin_com_pag = edo_cta_sin_igtf[
            (edo_cta_sin_igtf["porcentaje"] != 0.003)
        ].copy()
        comisiones = edo_cta_sin_com_pag.sort_values(
            by=["Fecha", "Referencia", "Monto"], ascending=[True, True, False]
        )
        comisiones["Monto_Ant"] = comisiones["Monto"].shift(1)
        comisiones["porcentaje"] = comisiones.apply(
            lambda x: round(x["Monto"] / x["Monto_Ant"], ndigits=3), axis=1
        )
        com_de_cobros = comisiones[(comisiones["porcentaje"] == -0.015)]
        # print("Comisiones de cobros\n", com_de_cobros.to_string())

        lista_df = [igtf_pagos, filtrar_igtf_rep_3, com_de_pagos, com_de_cobros]
        df_union_query = concat(lista_df).reset_index(drop=True)
        df_union_query["co_cta_ingr_egr"] = "6-2-01-01-0004"
        return df_union_query

    def get_mov_sin_identificar_libros(self):
        mov_sin_identificar = self.get_movimientos_bancarios_sin_identificar(mov="L")
        set_mov_identif_otros_meses = set(
            self.get_movimientos_bancarios_identificados_de_otros_meses()[
                "identif_mov_bco"
            ]
        )
        # se excluye los movimientos identificados de otros meses
        mov_sin_identificar = mov_sin_identificar[
            ~mov_sin_identificar["identif_mov_bco"].isin(set_mov_identif_otros_meses)
        ]
        return mov_sin_identificar


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
    # print(
    #     oConciliacion.get_comisiones_e_igtf_a_insertar(
    #         fecha_d="20250101", fecha_h="20250331"
    #     )
    # )
    # oConciliacion.insertar_movimientos_comisiones_igtf(
    #     fecha_d="20250301", fecha_h="20250331"
    # )
    # oConciliacion.insertar_movimientos_identificados(ultima_fecha="20250331")
    # print(oConciliacion.get_movimientos_actualizar_edo_cta())
    # movimientos sin identificar libros
    print(oConciliacion.get_mov_sin_identificar_libros())
    # print(datos)
