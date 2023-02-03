from airflow.decorators import dag
from airflow.models import Variable
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from method_1.utils import *
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

@dag(
    dag_id="Iyzico_etl_method1",
    schedule=None, #"*/20 * * * *", 
    tags=["Iyzico", "etl", "method1"],
    start_date=datetime(2020, 2, 2)
)
def dag():

    

    db = BaseHook.get_connection(Variable.get("iyzico_etl_source_connection_id"))
    engine = create_engine(f'postgresql+psycopg2://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}')


    start = DummyOperator(task_id="start")

    tasks = []
    import json
    for key, value in json.loads(Variable.get('method1_etl_configs')).items():
        tasks.append(etl_task_group(key, value, engine))

    healt = healt_check(engine)

    start >> tasks >> healt >> DummyOperator(task_id="end")

dag()
