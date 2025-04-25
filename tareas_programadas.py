import time
from datetime import datetime
from edo_cta_update import EdoCtaUpdate
from conciliacion import Conciliacion
from apscheduler.schedulers.background import BackgroundScheduler


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

    from conn.conexion import DatabaseConnector
    from dotenv import load_dotenv

    load_dotenv()
    # Para SQL Server
    datos_conexion = dict(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        base_de_datos=os.environ["DB_NAME_DERECHA_PROFIT"],
    )
    oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
    fecha_d = "20250101"
    fecha_h = "20250430"
    tareas_programadas = TareasProgramada(oConexion)
    tareas_programadas.run_tasks(fecha_d=fecha_d, fecha_h=fecha_h)
