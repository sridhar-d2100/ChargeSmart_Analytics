
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.papermill.operators.papermill import PapermillOperator


default_arg = {
    'owner':"sridhar",
    'start_data':day_ago(0),
}

@ DAG(
    default_arg = default_arg
    schedule_interval='@daily'
    tags=['do_inside'],  
)

def do_inside():

      run_notebook = PapermillOperator(
        task_id="run_notebook",
        input_nb="/path/to/your_notebook.ipynb",  # Path to your notebook
        output_nb="/path/to/output_notebook.ipynb",  # Path for saving the executed notebook
        parameters={"param": "value"},  # Optional parameters to pass to the notebook
    )
do_inside()