from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import pymssql, os
from jinja2 import FileSystemLoader
from jinja2.environment import Environment
import smtplib
from email.mime.multipart import  MIMEMultipart
from email.mime.text import MIMEText
from utility import utility


UTL= utility()

conn = pymssql.connect(host='192.168.3.40', user=UTL.user, password=UTL.password, database='ESIDB')
#conn = pymssql.connect(str.strip())

default_args = {
    'owner': 'IXI',
    'depends_on_past': False,
    'email': ['mehrdadn@integenx.com.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('IncominQC', description='Check every 15 min to see if there is any incoming QC',
          schedule_interval='*/15 * * * *',
          start_date=datetime(2017, 10, 4), catchup=False)



s = """
USE ESIDB
Select top 6 POFRT.PART_ID, ICFPM.PART_DESC, POFRT.PART_UM, POFRT.PO_ID,
POFRT.LOT_ID, POFRT.QUANTITY, POFRT.CREATED_BY, POFRT.TIME_LAST_UPDT
FROM POFRT
INNER JOIN ICFPM ON POFRT.PART_ID = ICFPM.PART_ID WHERE POFRT.ACTION_TYPE = 'PQ'
--AND DAY(POFRT.DATE_CREATED) = DAY(GETDATE()) AND MONTH(POFRT.DATE_CREATED) = MONTH(GETDATE())
--AND YEAR(POFRT.DATE_CREATED) = YEAR(GETDATE()) AND DATEDIFF(MI,POFRT.DATE_CREATED,GETDATE())<15

"""



def load():
    resaults_as_dict = []
    try:
        cur = conn.cursor()
        cur.execute(s)
        for row in cur:
            val = row
            res_as_dict = {
                'PART_ID': val[0].strip(),
                'PART_DESC': val[1].strip(),
                'PART_UM': val[2],
                'PO_ID': val[3],
                'QUANTITY': val[4],
                'CREATED_BY': val[5],
                'TIME_LAST_UPDT': val[6],
            }
            resaults_as_dict.append(res_as_dict)
    except Exception as e:
        res_as_dict = {'ERROR': str(e)}
        resaults_as_dict.append(res_as_dict)
    finally:
        cur.close()
        conn.close()
        return resaults_as_dict


def branching(**context):

    resaults_as_dict = context['task_instance'].xcom_pull(task_ids='load_task')
    return 'dummy_task' if not bool(resaults_as_dict) else  'content_task'


def content(**context):

    resaults_as_dict = context['task_instance'].xcom_pull(task_ids='load_task')
    env = Environment(loader=FileSystemLoader(os.path.join("/root/airflow/dags/", "")))
    # env = Environment(loader=FileSystemLoader(os.path.join("C:\Python34\ixi_TBOX34\AIRFLOW", "")))
    template = env.from_string('{% extends "GeneralTemplate.html" %}'
                               '{% block content %}'
                               '<table class="table"><tr><th>PART_ID</th><th>PART_DESC</th><th>PART_UM</th><th>PO_ID</th><th>QUANTITY</th><th>CREATED_BY</th><th>TIME_LAST_UPDT</th></tr>'
                               '{% for row in rows %}<tr><td>{{row.PART_ID}}</td><td>{{row.PART_DESC}}</td><td>{{row.PART_UM}}</td><td>{{row.PO_ID}}</td><td>{{row.QUANTITY}}</td><td>{{row.CREATED_BY}}</td><td>{{row.TIME_LAST_UPDT}}</td></tr>{% endfor %}'
                               '</table>'
                               '{% endblock %}')
    html = template.render(rows=resaults_as_dict)
    return html


def email(**context):

    try:
        html = context['task_instance'].xcom_pull(task_ids='content_task')
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'IncominQC'
        msg['From'] = "mehrdadn@integenx.com"
        msg['To'] = UTL.recipients('IncominQC')#"mehrdadn@integenx.com" #leilae@integenx.com;
        msg['Body']= html
        body = MIMEText(html, 'html')
        msg.attach(body)
        s = smtplib.SMTP('IXI-EXCH.microchipbiotech.com')
        s.send_message(msg)
        s.quit()
        return html
    except Exception as e:
        print(e)




load_task = PythonOperator(task_id='load_task', python_callable=load, dag=dag)
branching_task = BranchPythonOperator(task_id='branching_task', python_callable=branching, dag=dag, provide_context=True)
dummy_task = DummyOperator(task_id='nothingtoemail', dag=dag)
content_task = PythonOperator(task_id='content_task', python_callable=content, dag=dag, provide_context=True)
email_task= PythonOperator(task_id='email_task', python_callable=email, dag=dag, provide_context=True)


branching_task.set_upstream(load_task)
content_task.set_upstream(branching_task)
dummy_task.set_upstream(branching_task)
content_task.set_downstream(email_task)
