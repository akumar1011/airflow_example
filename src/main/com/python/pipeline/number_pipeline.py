from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from com.config.logging_config import Airflow_Logger
from com.python.etl.first import First
from com.python.etl.second import Second
from com.python.etl.third import Third
from com.python.etl.fourth import Fourth
from com.python.etl.fifth import Fifth

af_logger = Airflow_Logger("airflow_pipeline.log")


def start():
    # logging.info("This DAG is starting.")
    af_logger.info("This DAG is starting.")
    print("This is Starting.")


def first():
    _first = First()
    af_logger.info("first() starting.")
    return _first.load()


def second():
    _second = Second()
    af_logger.info("second() starting.")
    return _second.load()


def third():
    _third = Third()
    af_logger.info("third() starting.")
    return _third.load()


def fourth():
    _fourth = Fourth()
    af_logger.info("fourth() starting.")
    return _fourth.load()


def fifth():
    _fifth = Fifth()
    af_logger.info("fifth() starting.")
    return _fifth.load()


dag = DAG("Demo-Pipeline",
          description="This is a demonstration DAG",
          schedule_interval="0 20 * * *",
          start_date=days_ago(2),
          catchup=False
          )
start = PythonOperator(task_id="start", python_callable=start, dag=dag)
first = PythonOperator(task_id="first", python_callable=first, dag=dag)
second = PythonOperator(task_id="second", python_callable=second, dag=dag)
third = PythonOperator(task_id="third", python_callable=third, dag=dag)
fourth = PythonOperator(task_id="fourth", python_callable=fourth, dag=dag)
fifth = PythonOperator(task_id="fifth", python_callable=fifth, dag=dag)

"""will execute in sequence """
# start >> first >> second >> third , fourth >> fifth
"""second and third will run parallel"""
start >> first >> [second, third] >> fourth >> fifth
