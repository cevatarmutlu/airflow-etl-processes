from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import json
from datetime import datetime

from method_2.extract import get_extract_tasks_as_dict
from method_2.transform import *
from method_2.load import load_tasks
from method_2.utils import *
from method_2.truncate import truncate_tasks

@dag(
    dag_id="etl_method2",
    schedule="*/20 * * * *", 
    tags=["etl", "method2"],
    start_date=datetime(2023, 2, 2)
)
def dag():

    engine_source = PostgresHook(Variable.get("source_connection")).get_sqlalchemy_engine()
    engine_target = PostgresHook(Variable.get("target_connection")).get_sqlalchemy_engine()
    

    start_task = start()

    extract_tasks_dict = get_extract_tasks_as_dict(engine_source)

    tasks = []
    for key, value in json.loads(Variable.get("method2_transform_mapping")).items():

        transform_task = transforms_tasks[key.strip()]
        transformed_data = transform_task(
            *[extract_tasks_dict[i.strip().replace(".", "_")] for i in value.split(",")]
        )
        truncate_task = truncate_tasks[key.strip()](engine_target, key.strip(), transformed_data)
        load_task_result = load_tasks[key.strip()](truncate_task, engine_target, key.strip())
        
        tasks.append(load_task_result)

    
    healt = healt_check()

    start_task >> list(extract_tasks_dict.values())
    tasks >> healt >> EmptyOperator(task_id="end")

dag()


