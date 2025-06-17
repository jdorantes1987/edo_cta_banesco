import sys

sys.path.append("..\\profit")
from data.mod.banco.mov_bancarios_oper import MovimientosBacariosOperaciones
from data.mod.compra.cie import CuentasIngresoEgreso
from numpy import where
from pandas import DataFrame, concat, merge

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
        """
        Identifica y recupera los movimientos bancarios que coinciden con las entradas del estado de cuenta.

        Este método compara los identificadores de las transacciones bancarias con los del estado de cuenta
        para determinar qué transacciones son comunes entre ambos conjuntos de datos.

        Returns:
            pandas.DataFrame: Un DataFrame que contiene las transacciones bancarias que han sido
            identificadas como coincidentes con las entradas del estado de cuenta.
        """
        conjunto_edo_cta = set(self.mov_edo_cta["identif_edo_cta"])
        movimientos_bancarios = self.mov_bancarios
        conjunto_mov_bco = set(movimientos_bancarios["identif_mov_bco"])
        conjunto_identificados = (
            conjunto_edo_cta & conjunto_mov_bco
        )  # Determina qué movimientos se cruzan
        movimientos_bancarios_identificados = movimientos_bancarios[
            movimientos_bancarios["identif_mov_bco"].isin(conjunto_identificados)
        ]
        return movimientos_bancarios_identificados

    def get_movimientos_bancarios_sin_identificar(self, **kwargs):
        """
        Identifica y recupera las transacciones bancarias que no están conciliadas con el estado de cuenta.

        Este método compara dos conjuntos de transacciones: transacciones bancarias y transacciones del estado de cuenta.
        Dependiendo del parámetro `mov`, identifica las transacciones que no están en los libros
        o que no están en el estado de cuenta bacario.

        Args:
            **kwargs: Argumentos de palabra clave arbitrarios.
            - mov (str): Una bandera para determinar el tipo de movimiento a identificar.
              Si es "L", identifica las transacciones pendientes por identifica en libros que no están presentes en el estado de cuenta.
              De lo contrario, identifica las transacciones del estado de cuenta que no están presentes en los registros bancarios.

        Returns:
            pandas.DataFrame: Un DataFrame que contiene las transacciones no conciliadas según el criterio especificado.
        """
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
        """
        Identifica y recupera transacciones bancarias de meses anteriores al coincidir
        patrones específicos de referencia entre el estado de cuenta y los libros.
        Este método procesa las transacciones bancarias no identificadas tanto del
        estado de cuenta como de los libros, extrae componentes específicos de las
        referencias y las compara para identificar transacciones que correspondan a
        meses anteriores.
        Returns:
            pandas.DataFrame: Un DataFrame que contiene las transacciones bancarias
            identificadas de meses anteriores. El DataFrame resultante incluye datos
            combinados tanto del estado de cuenta como de los libros, con columnas
            con sufijos "_edo_cta" (estado de cuenta) y "_libro" (libros).
        """
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
        """
        Recupera y procesa los movimientos bancarios para actualizar el estado de cuenta.
        Este método consolida varios tipos de movimientos bancarios, incluyendo movimientos
        identificados, movimientos de otros meses, movimientos no identificados y comisiones
        (por ejemplo, IGTF). Estandariza los nombres de las columnas, asigna tipos de movimientos
        y filtra duplicados o entradas irrelevantes para preparar un conjunto de datos unificado
        para la actualización del estado de cuenta.
        Retorna:
            pandas.DataFrame: Un DataFrame que contiene los movimientos bancarios consolidados
            y ordenados con las siguientes columnas:
            - fecha: La fecha del movimiento.
            - mov_num: El número del movimiento bancario.
            - cie: El estado o tipo de cuenta.
            - concepto: La descripción o concepto del movimiento.
            - referencia: El número de referencia del movimiento.
            - monto: El monto del movimiento.
            - identif_mov_bco: El identificador único del movimiento bancario.
            - tipo_p: El tipo de movimiento (por ejemplo, B1, B2, B3, B4).
        Notas:
            - Los movimientos identificados se categorizan como "B1".
            - Los movimientos identificados de otros meses se categorizan como "B2".
            - Los movimientos no identificados se categorizan como "B3".
            - Las comisiones e IGTF se categorizan como "B4".
            - Los movimientos de otros meses y las comisiones se excluyen de los movimientos
              no identificados para evitar duplicados.
            - El DataFrame resultante se ordena por "fecha" y "mov_num" en orden ascendente.
        """
        columnas_base = [
            "fecha",
            "mov_num",
            "cie",
            "concepto",
            "referencia",
            "monto",
            "identif_mov_bco",
            "fecha_otros_meses",
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

        # Se establece el nombre de la columna de fecha
        mov_identificados["fecha_otros_meses"] = ""

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
                "fecha",
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
                "fecha": "fecha_otros_meses",
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

        # Se establece el nombre de la columna de fecha
        comisiones_igtf["fecha_otros_meses"] = ""

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

        # Se establece el nombre de la columna de fecha
        mov_sin_identificar["fecha_otros_meses"] = ""

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

        # se excluye los movimientos identificados de otros meses de las comisiones
        comisiones_igtf = comisiones_igtf[
            ~comisiones_igtf["identif_mov_bco"].isin(set_mov_identif_otros_meses)
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
        """
        Valida y filtra los movimientos bancarios para ser insertados en la BD del sistema Profit.

        Este método recupera los movimientos bancarios no identificados de tipo "E" y los une
        con los datos de cuentas de ingreso y egreso. Actualiza el campo "Comentarios" basado
        en el campo "Descripción" y filtra los datos resultantes para incluir solo las filas
        donde el código de cuenta no sea nulo y el campo "Contabilizar" esté configurado como "SI".

        Retorna:
            pandas.DataFrame: Un DataFrame filtrado que contiene los movimientos bancarios validados
            listos para su inserción. El DataFrame incluye solo las filas donde:
            - El campo "co_cta_ingr_egr" no es nulo.
            - El campo "Contabilizar" (insensible a mayúsculas) es igual a "SI".
        """
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
        """
        Inserta movimientos bancarios identificados en la base de datos del sistema Profit.

        Este método procesa un conjunto de movimientos bancarios, les asigna
        identificadores únicos y los inserta en la base de datos. También
        confirma la inserción después de procesar todos los movimientos.

        Args:
            ultima_fecha (str): La última fecha a considerar para recuperar el
            último ID de movimiento bancario en Profit.

        Returns:
            pandas.DataFrame: Un DataFrame que contiene los datos de los
            movimientos que fueron procesados e insertados.
        """
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
        """
        Inserta movimientos de comisiones e IGTF (Impuesto a las Grandes Transacciones Financieras)
        no registrados en la base de datos Profit para un rango de fechas especificado.

        Este método recupera los datos de comisiones e IGTF que no han sido registrados, los filtra
        por el rango de fechas proporcionado y los inserta como nuevos movimientos bancarios en la base de datos.

        Args:
            fecha_d (datetime): La fecha de inicio del rango para filtrar los movimientos.
            fecha_h (datetime): La fecha de fin del rango para filtrar los movimientos.

        Returns:
            pandas.DataFrame: Un DataFrame que contiene los datos de comisiones e IGTF filtrados que fueron procesados.

        Raises:
            Cualquier excepción generada por las operaciones de base de datos subyacentes o el procesamiento de datos.

        Notas:
            - El método asume que el objeto `oConciliacion` proporciona un método `get_comisiones_e_igtf_sin_registrar`
              para recuperar los datos de comisiones e IGTF no registrados.
            - La clase `MovimientosBacariosOperaciones` se utiliza para manejar las operaciones de base de datos
              relacionadas con los movimientos bancarios.
            - Se llama al método `new_movbanco` para insertar cada movimiento, y la inserción se confirma al final
              utilizando `confirmar_insercion_movimientos_bancarios`.
            - La descripción de cada movimiento se trunca a 159 caracteres si excede esta longitud.
            - El método imprime el ID, la descripción y el monto de cada movimiento insertado para fines de registro.
        """
        datos = oConciliacion.get_comisiones_e_igtf_sin_registrar()
        # Filtra entre fechas
        datos = datos[
            (datos["Fecha"] >= fecha_d)
            & (datos["Fecha"] <= fecha_h)
            & (datos["Contabilizar"].str.upper() != "NO")
        ]
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
        """
        Recupera las comisiones e IGTF para ser registrados en la base de datos Profit.

        Este método filtra y devuelve las comisiones e IGTF que están pendientes
        de registro comparándolas con los movimientos bancarios no identificados.

        Retorna:
            pandas.DataFrame: Un DataFrame que contiene las comisiones e IGTF
            pendientes de registro. El DataFrame incluye únicamente aquellas
            transacciones cuyos identificadores coinciden con los movimientos
            bancarios no identificados.
        """
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
        """
        Extrae y procesa datos financieros para identificar transacciones relacionadas con IGTF
        (Impuesto a las Grandes Transacciones Financieras) y comisiones del estado de cuenta bancario
        dentro de un rango de fechas especificado.

        Args:
            fecha_d (datetime): La fecha de inicio para filtrar los movimientos del estado de cuenta.
            fecha_h (datetime): La fecha de fin para filtrar los movimientos del estado de cuenta.
            **kwargs: Argumentos adicionales (no utilizados en la implementación actual).

        Returns:
            DataFrame: Un DataFrame consolidado que contiene lo siguiente:
            - Pagos de IGTF con 4 repeticiones en referencias bancarias.
            - Cobros de IGTF con 3 repeticiones en referencias bancarias.
            - Comisiones de pagos.
            - Comisiones de cobros.
            - Una nueva columna `co_cta_ingr_egr` para incluir la cuenta de ingresos y egresos "6-2-01-01-0004".

        Notas:
            - Filtra transacciones con fechas no nulas y no vacías dentro del rango especificado.
            - Identifica pagos de IGTF basándose en patrones específicos de repeticiones y porcentajes.
            - Identifica cobros de IGTF al cruzar referencias y filtrar por porcentaje y número de repeticiones.
            - Identifica comisiones de pagos basándose en umbrales específicos de porcentaje.
            - Identifica comisiones de cobros basándose en un porcentaje fijo (-0.015).
            - Combina todas las transacciones identificadas en un único DataFrame para análisis posterior.
        """
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
        com_de_pagos = edo_cta_sin_igtf[(edo_cta_sin_igtf["porcentaje"] == 0.003)]
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
        """
        Recupera los movimientos bancarios no identificados registrados en libros y no identificados en el estado de cuenta,
        excluyendo aquellos que han sido identificados en otros meses.

        Este método obtiene los movimientos bancarios marcados como no identificados
        (mov="L") y excluye los movimientos que ya han sido identificados en otros meses.

        Retorna:
            pandas.DataFrame: Un DataFrame que contiene los movimientos bancarios
            no identificados en los libros, excluyendo los identificados en otros meses.
        """
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
    import os

    from dotenv import load_dotenv

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    load_dotenv(override=True)

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    db = DatabaseConnector(sqlserver_connector)
    f_desde = "20250101"
    f_hasta = "20250630"
    oConciliacion = Conciliacion(
        conexion=db, sheet_name_edo_cta="2025", fecha_d=f_desde, fecha_h=f_hasta
    )
    # print(oConciliacion.get_comisiones_e_igtf_sin_registrar())
    # print(oConciliacion.validacion_movimientos_a_insertar())
    # oConciliacion.insertar_movimientos_comisiones_igtf(
    #     fecha_d="20250401", fecha_h="20250430"
    # )
    # oConciliacion.insertar_movimientos_identificados(ultima_fecha="20250430")
    # print(oConciliacion.get_movimientos_actualizar_edo_cta())
    # movimientos sin identificar libros
    print(oConciliacion.get_mov_sin_identificar_libros())
    # print(datos)
