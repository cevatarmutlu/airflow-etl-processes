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

    engine = PostgresHook(Variable.get("source_connection")).get_sqlalchemy_engine()


    start = EmptyOperator(task_id="start")

    tasks = []
    import json
    for key, value in json.loads(Variable.get('method1_etl_configs')).items():
        tasks.append(etl_task_group(key, value, engine))

    healt = healt_check(engine)

    start >> tasks >> healt >> EmptyOperator(task_id="end")

dag()
