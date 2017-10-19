from airflow import DAG
from airflow.operators import bash_operator
from airflow.operators.bash_operator import BashOperator
from builtins import range
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pymssql

server = "192.168.3.40"
user = "sysdba"
password = "e$1s_s"
database = "ESIDB"
# Following are defaults which can be overridden later on
default_args = {
    'owner': 'lalit.bhatt',
    'depends_on_past': False,
    'start_date': datetime(2017, 10, 10),
    'email': ['lalit.bhatt@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# dag = DAG('HelloWorld', default_args=default_args)



def insert_TO_TB():
    # return 'Hello world!'
    conn = pymssql.connect(host='192.168.3.40', user='sysdba', password='e$1s_s', database='ESI_DATA')
    desc = 'this isb testNEW'
    s = 'INSERT INTO [ESI_DATA].[dbo].[IXI_J1]([F1],[F2],[F3]) VALUES (1,' + repr(desc) + ', CAST(GETDATE() as date))'
    cur = conn.cursor()
    cur.execute(s)
    cur.close()
    conn.commit()
    conn.close()
dag = DAG('Insert_TO_DB', description='Simple tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 10, 10), catchup=False)

# t1, t2, t3 and t4 are examples of tasks created using operators
t1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag)
t2 = BashOperator(
    task_id='task_2',
    bash_command='echo "Hello World from Task 2"',
    dag=dag)
t3 = BashOperator(
    task_id='task_3',
    bash_command='echo "Hello World from Task 3"',
    dag=dag)
t4 = PythonOperator(
    task_id='insert',
    python_callable=insert_TO_TB, dag=dag)
t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t2)
t4.set_upstream(t3)
