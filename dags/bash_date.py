import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

###############################################################################
#                         股票機器人 教學示範 自動報時                          #
###############################################################################

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['user@tmr.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,我都搶不到
    # 'trigger_rule': u'all_success'
}


dag = DAG(
    'tutorial',
    default_args=default_args,
    description='my first DAG',
    schedule_interval='10 * * * * *')

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)


