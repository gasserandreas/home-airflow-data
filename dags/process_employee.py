import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="process_employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
  create_employees_table = SQLExecuteQueryOperator(
    task_id="create_employees_table",
    conn_id="airflow_data",
    sql="""
      CREATE TABLE IF NOT EXISTS employees (
        "Serial Number" NUMERIC PRIMARY KEY,
        "Company Name" TEXT,
        "Employee Markme" TEXT,
        "Description" TEXT,
        "Leave" INTEGER
      );
    """,
  )

  create_employees_temp_table = SQLExecuteQueryOperator(
    task_id="create_employees_temp_table",
    conn_id="airflow_data",
    sql="""
      DROP TABLE IF EXISTS employees_temp;
      CREATE TABLE employees_temp (
          "Serial Number" NUMERIC PRIMARY KEY,
          "Company Name" TEXT,
          "Employee Markme" TEXT,
          "Description" TEXT,
          "Leave" INTEGER
    );
    """,
  )

  @task
  def echo_something():
    print("something")

  @task
  def get_data():
    data_path = "/opt/airflow/data/employees.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)

    url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"

    response = requests.request("GET", url)

    with open(data_path, "w") as file:
      file.write(response.text)

    postgres_hook = PostgresHook(postgres_conn_id="airflow_data")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(data_path, "r") as file:
      cur.copy_expert(
        "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
        file,
      )
    
    conn.commit()

  @task
  def merge_data():
    query = """
      INSERT INTO employees
      SELECT *
      FROM (
        SELECT DISTINCT *
        FROM employees_temp
      ) t
      ON CONFLICT ("Serial Number") DO UPDATE
      SET
        "Employee Markme" = excluded."Employee Markme",
        "Description" = excluded."Description",
        "Leave" = excluded."Leave";
    """

    try:
      postgres_hook = PostgresHook(postgres_conn_id="airflow_data")
      conn = postgres_hook.get_conn()
      cur = conn.cursor()
      cur.execute(query)
      conn.commit()
      return 0
    except Exception as e:
      print(e)
      return 1
  
  [create_employees_table, create_employees_temp_table] >> echo_something() >> get_data() >> merge_data()


dag = ProcessEmployees()