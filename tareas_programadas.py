import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from conciliacion import Conciliacion
from edo_cta_update import EdoCtaUpdate


class TareasProgramada:
    def __init__(self, conexion):
        self.conexion = conexion

    def actualizar_colores_edo_cta(self, **kwargs):
        oEdoCtaUpdate = EdoCtaUpdate(self.conexion)
        print(
            f"actualizar_colores_edo_cta: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        )
        oEdoCtaUpdate.update_edo_cta("2025", **kwargs)

    def insertar_movimientos_identificados(self, **kwargs):
        fecha_d = kwargs.get("fecha_d", "NULL")
        fecha_h = kwargs.get("fecha_h", "NULL")
        tConciliacion = Conciliacion(
            conexion=self.conexion,
            sheet_name_edo_cta="2025",
            fecha_d=fecha_d,
            fecha_h=fecha_h,
        )
        print(
            f"insertar_movimientos_identificados: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        )
        tConciliacion.insertar_movimientos_identificados(ultima_fecha="20250430")

    def run_tasks(self, **kwargs):
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            self.actualizar_colores_edo_cta,
            "interval",
            kwargs=kwargs,
            hours=3,
            minutes=0,
            seconds=30,
        )

        scheduler.add_job(
            self.insertar_movimientos_identificados,
            "interval",
            kwargs=kwargs,
            hours=3,
            minutes=0,
            seconds=10,
        )

        scheduler.start()
        print(
            f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} tareas iniciadas. Actualizando cada {scheduler.get_jobs()[0].trigger}"
        )

        try:
            # Mantener el script en ejecución
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            print("Tarea programada detenida.")


if __name__ == "__main__":
    import os
    import sys

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector
    from dotenv import load_dotenv

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
    oConexion = DatabaseConnector(db)
    fecha_d = "20250101"
    fecha_h = "20250430"
    tareas_programadas = TareasProgramada(oConexion)
    tareas_programadas.run_tasks(fecha_d=fecha_d, fecha_h=fecha_h)
