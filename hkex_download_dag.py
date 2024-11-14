
import hkex.downloadHkexFiles as dl
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from utils.misc import GetEmailReceipts, CallFuncWithTaskExecDate, GetTargetTime



root_path = Variable.get("local_root_path")
save_path = os.path.join(root_path, 'data/')

ls_path = 'ListOfSecurities'
ls_dir =  os.path.join(save_path, ls_path)

default_args = {
    'email': GetEmailReceipts(),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id="hkex_dag",
          description="ETL processing from HKEx/HSI websites DAG",
          start_date=datetime(year=2024,month=11,day=13),
          schedule_interval='0 7 * * 1-5',
          tags=['hkex-dags'],
          default_args=default_args,
          catchup=False)

with TaskGroup(group_id='schedule_0715', dag=dag) as schedule_0715:
    dts = DateTimeSensorAsync(
    task_id="dt_async",
    dag=dag,
    target_time=GetTargetTime(7, 15)
    )
    with TaskGroup(group_id='HKEx_ListOfSecurities', dag=dag) as list_of_sec:

        with TaskGroup(group_id='English', dag=dag) as english:
            extract1 = PythonOperator(
            task_id="Download",
            dag=dag,
            python_callable=CallFuncWithTaskExecDate,
            op_kwargs={'Path': ls_dir, 'Func':dl.HKEx_ListOfSecuritiesDownload})

            extract1
    dts >> list_of_sec

schedule_0715