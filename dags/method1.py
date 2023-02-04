from airflow.decorators import dag
from airflow.models import Variable
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from method_1.utils import *
from airflow.providers.postgres.hooks.postgres import PostgresHook 

@dag(
    dag_id="Iyzico_etl_method1",
    schedule="*/20 * * * *", 
    tags=["Iyzico", "etl", "method1"],
    start_date=datetime(2023, 2, 2)
)
def dag():

    engine_source = PostgresHook(Variable.get("source_connection")).get_sqlalchemy_engine()
    engine_target = PostgresHook(Variable.get("target_connection")).get_sqlalchemy_engine()


    start = EmptyOperator(task_id="start")

    tasks = []
    import json
    for key, value in json.loads(Variable.get('method1_etl_configs')).items():
        tasks.append(etl_task_group(key, value, engine_source, engine_target))

    healt = healt_check()

    start >> tasks >> healt >> EmptyOperator(task_id="end")

dag()
