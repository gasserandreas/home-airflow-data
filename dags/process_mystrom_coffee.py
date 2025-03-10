import datetime

import json
import os

import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
  dag_id="process_mystrom_coffee",
  schedule_interval="*/5 * * * *",
  start_date=datetime.datetime(2021, 1, 1),
  catchup=False,
  tags=["mystrom", "coffee"],
)
def ProcessMyStromCoffee():
  DATA_PATH_DIR = "/opt/airflow/data/mystrom_coffee_report/"

  create_mystrom_coffee_table = SQLExecuteQueryOperator(
    task_id="create_mystrom_coffee_table",
    conn_id="airflow_data",
    sql="""
      CREATE TABLE IF NOT EXISTS mystrom_coffee_report (
      "id" BIGSERIAL PRIMARY KEY,
      "power" NUMERIC(10, 2),
      "ws" NUMERIC(10, 2),
      "relay" BOOLEAN,
      "temperature" NUMERIC(5, 2),
      "boot_id" VARCHAR(50),
      "energy_since_boot" NUMERIC(15, 2),
      "time_since_boot" BIGINT,
      "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    """,
  )
  
  @task
  def extract_data():
    mystrom_coffee_dag_var = Variable.get("mystrom_coffee_dag", deserialize_json=True)
    url = f"http://{mystrom_coffee_dag_var['ip_address']}/report"
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)

    data_path = DATA_PATH_DIR + "extract.json"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    
    print(response.text)

    with open(data_path, "w") as file:
      file.write(response.text)

  @task
  def transform_data():
    pass
  
  @task
  def load_data():
    data_path = DATA_PATH_DIR + "extract.json"

    with open(data_path, "r") as file:
      data = json.load(file)
      print(data)

      postgres_hook = PostgresHook(postgres_conn_id="airflow_data")
      connection = postgres_hook.get_conn()
      cur = connection.cursor()
      cur.execute(
        """
        INSERT INTO mystrom_coffee_report (power, ws, relay, temperature, boot_id, energy_since_boot, time_since_boot)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
          data["power"],
          data["Ws"],
          data["relay"],
          data["temperature"],
          data["boot_id"],
          data["energy_since_boot"],
          data["time_since_boot"],
        ),
      )
      connection.commit()
      cur.close()
      connection.close()

  create_mystrom_coffee_table >> extract_data() >> transform_data() >> load_data()

dag = ProcessMyStromCoffee()