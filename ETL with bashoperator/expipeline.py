from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# create the DAG arguments
default_args = {
    'owner': 'Ramesh',
    'start_date': days_ago(0),
    'email': ['loing9900@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#create the DAG definition
dag = DAG(
    'etl-log-processing-dag',
    default_args=default_args,
    description= 'My first DAG',
    schedule_interval = timedelta(days=1)
)

#define the 1st task
download = BashOperator (
    task_id = 'download',
    bash_command = 'wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
    dag = DAG
)

#define the 2nd task
extract = BashOperator (
    task_id = 'extract',
    bash_command = 'cut -f1,4 -d"#" web-server-access-log.txt > extracted.txt',
    dag = DAG
)

#define the 3rd task
transform = BashOperator (
    task_id = 'transform',
    bash_command = 'tr "[a-z]" "[A-Z]" < extracted.txt > capitalized.txt',
    dag = DAG
)

#load task
load = BashOperator (
    task_id= 'load',
    bash_command = 'zip log.zip capitalized.txt',
    dag = DAG
)
#define the dependencies
download >> extract >> transform >> load 
