from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from method_2.extract import get_extract_tasks_as_dict
from method_2.transform import *
from method_2.load import get_laod_task_as_dict
from method_2.utils import *


@dag(
    dag_id="Iyzico_etl_method2",
    schedule=None, #"*/20 * * * *", 
    tags=["Iyzico", "etl"],
    start_date=datetime(2020, 2, 2)
)
def dag():
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine
    from airflow.models import Variable

    db = BaseHook.get_connection(Variable.get("iyzico_etl_source_connection_id"))
    engine = create_engine(f'postgresql+psycopg2://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}')
    

    start_task = start()

    extract_tasks_dict = get_extract_tasks_as_dict()

    # Transform
    accounting = transform_accounting_unit_data_mart(**extract_tasks_dict)
    marketing = transform_marketing_unit_data_mart(**extract_tasks_dict)
    transform_task = {
        "accounting_unit_data_mart": accounting,
        "marketing_unit_data_mart": marketing
    }
    
    load_task = get_laod_task_as_dict(transform_task, engine)
    healt = healt_check(engine)

    start_task >> list(extract_tasks_dict.values())
    load_task >> healt >> DummyOperator(task_id="end")

dag()
