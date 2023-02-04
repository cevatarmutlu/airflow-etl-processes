from airflow.decorators import dag, task
import subprocess
import logging

@task
def start():
    process = subprocess.Popen(["df", "-H"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = process.communicate()
    logging.info("\n" + out.decode("utf-8"))

@task
def healt_check():
    
    process = subprocess.Popen(["df", "-H"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    

    out, err = process.communicate()
    logging.info("\n" + out.decode("utf-8"))