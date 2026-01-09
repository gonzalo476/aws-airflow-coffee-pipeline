from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id='test_simple',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test_v3"]
)
def test_simple_dag():
    
    @task
    def tarea_1():
        print("✅ Tarea 1 ejecutada correctamente (Airflow 3 SDK)")
        return "Hola desde tarea 1"

    @task
    def tarea_2(mensaje):
        print(f"✅ Tarea 2 recibió: {mensaje}")
        return "Todo funciona!"

    @task
    def tarea_3(resultado):
        print(f"✅ Tarea 3 finalizó: {resultado}")

    # Flujo
    resultado_1 = tarea_1()
    resultado_2 = tarea_2(resultado_1)
    tarea_3(resultado_2)

test_simple_dag()