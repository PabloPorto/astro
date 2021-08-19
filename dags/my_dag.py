from airflow import DAG;
from airflow.models import Variable;
from airflow.operators.python import PythonOperator;
from airflow.providers.postgres.operators.postgres import PostgresOperator;

from datetime import datetime, timedelta;

class CustomPostegresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')

def _extract(partner_name):
    print(partner_name)

with DAG(dag_id="my_dag",
     description="in charge to processing customer data",
     start_date=datetime(2021,10,10),
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=10),
     tags=["data_science"],
     catchup=False,
     max_active_runs=1) as dag:
     
     extract = PythonOperator(
         task_id="extract",
         python_callable=_extract,
         op_args=["{{ var.json.my_dag_partner.name }}"]
     ) 

     fetching_data = CustomPostegresOperator(
         task_id="fetching_data",
         sql="sql/MY_REQUEST.sql",
         parameters={
             'next_ds':'{{next_ds}}',
             'prev_ds':'{{prev_ds}}',
             'partner_name':'{{var.json.my_dag_partner.name }}'
         }
     )