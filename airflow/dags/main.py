from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable


from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='Project_1',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt/',
)

@dag(
    Schedule = None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['do_run'],    
)

def do_run():

    staging_data = DbtTaskGroup(
    group_id="staging_data",                      # Group ID for the task group, named "staging_data".
    project_config=DBT_PROJECT_CONFIG,            # Configuration of the DBT project, such as project paths, profiles, etc.
    profile_config=DBT_CONFIG,                    # DBT profile configuration, which contains connection details (e.g., to the data warehouse).
    render_config=RenderConfig(                   # Defines how the DBT task group will select models to run.
        load_method=LoadMode.DBT_LS,              # Load method for running models; in this case, it's DBT's list command (`dbt ls`).
        select=["path:models/staging"]            # Specifies which models to run, here it selects models located in the `models/staging` directory.
        ),
    )

     transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/transform"]
        ),
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='marts'):
        from script.soda.check_function import check

        return check(scan_name, checks_subpath)

     report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    staging_data >> transform_data >> check_transform() >> report


do_run()

