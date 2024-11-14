#import hkex.simpleProcess as sp
import os
import pendulum
import re

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

AIRFLOW_ENV_PROD = 'prod'
AIRFLOW_ENV_UAT = 'uat'

def GetEmailReceipts(uat_email=None):
    if uat_email is not None and GetAirflowEnv() == AIRFLOW_ENV_UAT:
        return uat_email
    email_list_str = Variable.get("email_receipts")
    email_list = re.sub('\'|\[\]', '', email_list_str).split(',')
    return email_list

def GetAirflowEnv():
    return Variable.get("airflow_env")

def CallFuncWithTaskExecDate(**kwargs):
    hktz = pendulum.timezone('Asia/Hong_Kong')
    utc_dt = kwargs['data_interval_end']
    hkt_dt = hktz.convert(utc_dt)
    #if kwargs.get('TMinus1'):
        #hkt_dt = sp.GetPrevTradingDay(hkt_dt)
    func = kwargs.pop('Func')
    kwargs['ExecDate'] = hkt_dt
    status = func(**kwargs)
    if status == 0:
        raise AirflowSkipException
    return status


