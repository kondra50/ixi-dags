from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Dag is returned by a factory method
def load(Invoice):
  print(Invoice)


def sub_dag(parent_dag_name, child_dag_name,start_date,Invoices_as_dict):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    start_date=start_date,
  )

  for item in Invoices_as_dict:
    content_task = PythonOperator(task_id='content_task', python_callable=load(item), dag=dag)




  # dummy_operator = DummyOperator(
  #   task_id='dummy_task',
  #   dag=dag,
  # )

  return dag