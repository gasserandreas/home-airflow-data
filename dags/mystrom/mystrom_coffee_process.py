import datetime

import json
import os

import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
  dag_id="mystrom_coffee_process",
  schedule_interval="*/5 * * * *",
  start_date=datetime.datetime(2021, 1, 1),
  catchup=False,
  tags=["mystrom", "coffee"],
)
def MyStromCoffeeProcess():
  DATA_PATH_DIR = "/opt/airflow/data/mystrom_coffee_report/"
  MYSTROM_COFFEE_DAG_VAR = Variable.get("mystrom_coffee_dag", deserialize_json=True)
  
  def get_price_for_timestamp(timestamp, price_intervals):
    """
    Determine the price for a given timestamp based on the price intervals.
    
    Args:
        timestamp: A datetime object or string in ISO format
        price_intervals: List of price interval objects with start, end, and price fields
    
    Returns:
        float: The applicable price for the timestamp
    """
    # If timestamp is a string, convert it to datetime
    if isinstance(timestamp, str):
        timestamp = datetime.datetime.fromisoformat(timestamp)
        
    # Extract hours and minutes as a string in HH:MM format
    time_str = timestamp.strftime("%H:%M")
    
    # Find the applicable interval
    for interval in price_intervals:
        if interval["start"] <= time_str < interval["end"]:
            return interval["price"]
    
    # Handle edge case where time is exactly midnight (24:00/00:00)
    if time_str == "00:00" or time_str == "24:00":
        for interval in price_intervals:
            if interval["start"] == "00:00" or interval["end"] == "24:00":
                return interval["price"]
                
    # Default case if no interval found (shouldn't happen with complete day coverage)
    raise ValueError(f"No price interval found for time {time_str}")

  @task
  def extract_data():
    url = f"http://{MYSTROM_COFFEE_DAG_VAR['ip_address']}/report"
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

      price_intervals = MYSTROM_COFFEE_DAG_VAR["power_price_intervals"]
      current_timestamp = datetime.datetime.now()
      
      # Get current price
      price_per_kwh = get_price_for_timestamp(current_timestamp, price_intervals)

      postgres_hook = PostgresHook(postgres_conn_id="airflow_data")
      connection = postgres_hook.get_conn()
      cur = connection.cursor()
      cur.execute(
        """
        INSERT INTO mystrom_coffee_report (power, ws, relay, temperature, boot_id, energy_since_boot, time_since_boot, price_per_kwh)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
          data["power"],
          data["Ws"],
          data["relay"],
          data["temperature"],
          data["boot_id"],
          data["energy_since_boot"],
          data["time_since_boot"],
          price_per_kwh,
        ),
      )
      connection.commit()
      cur.close()
      connection.close()

  extract_data() >> transform_data() >> load_data()

dag = MyStromCoffeeProcess()