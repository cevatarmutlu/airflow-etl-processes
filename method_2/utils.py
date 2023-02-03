from airflow.decorators import dag, task
import subprocess
import logging

@task
def start():
    process = subprocess.Popen(["df", "-H"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = process.communicate()
    logging.info("\n" + out.decode("utf-8"))

@task
def healt_check(engine):
    import pandas as pd

    for table in ["public.sales_tx_t", "public.sales_t", "public.store_t", "data_marts.accounting_unit_data_mart", "data_marts.marketing_unit_data_mart"]:

        data = pd.read_sql(
            sql=f"SELECT * FROM {table}",
            con=engine
        )

        logging.info(f"{table} rows: {len(data.index)}")
    
    process = subprocess.Popen(["df", "-H"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    

    out, err = process.communicate()
    logging.info("\n" + out.decode("utf-8"))