from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import pandas as pd 
from airflow.hooks.mysql_hook import MySqlHook
from airflow import AirflowException

# Unpack the smaple_lists from database
def get_Mysql_data():

    def convert(a):
      return str(a).replace('(','').replace(')','').replace(',','').replace("'",'')

    mysql_hook = MySqlHook(mysql_conn_id='test_mysql')
    sql = "SELECT sample_id FROM actg_samples where active_state=1"
    records = mysql_hook.get_records(sql=sql)
    final = []
    for a in records:
        final.append(convert(a))
    return final

# create dags and return as dag_object
def create_dag(dag_id, schedule, dag_number, default_args, **context):
    
    # define tasks in each creating dags
    def check_panel(sample_name, **context):
      check_panel = 'haha'
      if check_panel == 'Onco2M7':
          return ['check_DNA_Conc','check_RNA_Conc']
      elif check_panel == 'Onco2M7':
          return 'check_DNA_Conc'
      elif check_panel != 'Onco2M7':
          return 'check_RNA_Conc'

    def check_DNA_Conc(sample_name, **context):
      checkDNA = True
      if checkDNA:
          return 'check_Library_Conc'
      else:
          return 'no_DNA_conc'

    def check_RNA_Conc(sample_name, **context):
      checkRNA = True
      if checkRNA:
          return 'check_Library_Conc'
      else:
          return 'no_RNA_conc'


    def check_Library_Conc(sample_name, **context):
        checkLibrary = False
        if checkLibrary:
            return "check_sequence"
        else:
            raise AirflowException

            
      
    def check_sequence(sample_name, **context):
        checSequence = True
        if checSequence:
            return "finish"
        else:
            return 'no_sequence_conc'
      
    def finish(sample_name, **context):
        print("ok")

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        get_sample_data = PythonOperator(
            task_id='get_Mysql_data',
            python_callable = get_Mysql_data
        )

        check_panel = BranchPythonOperator(
            task_id='check_panel',
            python_callable = check_panel,
            op_args=[sample_name],
            provide_context = True,
            dag_number=dag_number
        )

        check_DNA_Conc_task = BranchPythonOperator(
            task_id='check_DNA_Conc',
            python_callable = check_DNA_Conc,
            op_args=[sample_name],
            provide_context = True,
            dag_number=dag_number
        )

        check_RNA_Conc_task = BranchPythonOperator(
            task_id='check_RNA_Conc',
            python_callable = check_RNA_Conc,
            op_args=[sample_name],
            provide_context = True,
            dag_number=dag_number
        )
           
        check_Library_Conc_task = BranchPythonOperator(
            task_id='check_Library_Conc',
            python_callable= check_Library_Conc,
            op_args=[sample_name],
            provide_context = False,
            dag_number=dag_number
        )
        
        check_check_sequence_task = BranchPythonOperator(
            task_id='check_sequence',
            python_callable = check_sequence,
            op_args=[sample_name],
            provide_context = True,
            dag_number=dag_number
        )
        
        finish_task = PythonOperator(
            task_id='finish',
            python_callable = finish,
            op_args=[sample_name],
            provide_context = True,
            dag_number=dag_number
        )
        do_nothing = DummyOperator(task_id='no_do_nothing')
        no_XNA_conc = DummyOperator(task_id='no_XNA_conc')
        no_library_conc = DummyOperator(task_id='no_library_conc')
        no_sequence_conc = DummyOperator(task_id='no_sequence_conc')

        # define_orders
        check_panel >> check_DNA_Conc_task
        check_panel >> check_RNA_Conc_task

        check_DNA_Conc_task >> check_Library_Conc_task
        check_RNA_Conc_task >> check_Library_Conc_task
        
        check_Library_Conc_task >> check_check_sequence_task
        check_Library_Conc_task >> no_library_conc
        
        check_check_sequence_task >> finish_task
        check_check_sequence_task >> no_sequence_conc

    return dag
# sample_name_list = context['task_instance'].xcom_pull(dag_id='get_Mysql_data',task_ids='get_Mysql_data')
sample_name_list = get_Mysql_data()
# sample_name_list = ["AA-20-00001", "AA-20-00002","AA-20-00003","AA-20-00004","AA-20-00005","AA-20-00006","AA-20-00010"]
for n,sample_name in enumerate(sample_name_list):

    dag_id = sample_name
    default_args = {
        'owner': 'BruceChen',
        'start_date': datetime(2020, 3, 10),
        'schedule_interval': '@daily',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
    is_paused_upon_creation=False
    schedule = '@daily'

    dag_number = n

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)