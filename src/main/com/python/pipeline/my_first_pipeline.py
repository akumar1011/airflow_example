from datetime import datetime
import time

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2)
}


def printDAG():
    print("This DAG has started-----------!")


"""There are two ways to define DAG"""
"""
Note: catchup=False- Instruct the scheduler to only create a DAG Run for the most current instance of the DAG interval series.
                        https://airflow.apache.org/docs/stable/scheduler.html?highlight=backfill#backfill-and-catchup
"""
"""Method 1"""
# with DAG(
#         dag_id='MyFirstDAG',
#         default_args=default_args,
#         schedule_interval='@daily',
#         catchup=False,
#         description='This DAG is a sample DAG'
# ) as MyFirstDAG:
#     printingDAG = PythonOperator(
#         task_id='printingDAG',
#         python_callable=printDAG
#     )
# printingDAG

"""Method 2"""
dag = DAG('MyFirstDAG',
          description='Simple tutorial DAG',
          schedule_interval='0 21 * * *',
          start_date=days_ago(2),
          catchup=False
          )
printingDAG = PythonOperator(task_id='printingDAG', python_callable=printDAG, dag=dag)
printingDAG


"""
General Guideline-
1. schedule_interval should be in DAG definition not in default_args
2. DAG name should not have any space in between

"""