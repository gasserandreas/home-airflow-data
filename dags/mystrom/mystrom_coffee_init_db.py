import datetime

import json
import os

import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
  dag_id="mystrom_coffee_init_db",
  # this DAG should only run manually
  schedule_interval=None,
  start_date=None,
  catchup=False,
  tags=["mystrom", "coffee"],
)
def MyStromCoffeeInitDB():
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
      "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      "price_per_kwh" NUMERIC(5, 2)
      );
    """,
  )

  create_mystrom_coffee_view_usage = SQLExecuteQueryOperator(
    task_id="create_mystrom_coffee_view_usage",
    conn_id="airflow_data",
    sql="""
      CREATE OR REPLACE VIEW mystrom_coffee_usage AS
      SELECT
        id,
        power AS current_power,
        ws AS average_watt_per_second,
        ws * EXTRACT (
          -- extract seconds
          EPOCH FROM (
            created_at - LAG(created_at) OVER (ORDER BY created_at)
          )
        ) as ws_dot_seconds,
        relay AS power_on,
        temperature,
        created_at::date as date,
        date_trunc('minutes', created_at::time) as time,
        price_per_kwh
      FROM mystrom_coffee_report
      ORDER BY date DESC, time DESC
    """,
  )

  create_mystrom_coffee_view_usage_hourly = SQLExecuteQueryOperator(
    task_id="create_mystrom_coffee_view_usage_hourly",
    conn_id="airflow_data",
    sql="""
      CREATE OR REPLACE VIEW mystrom_coffee_usage_hourly AS
      SELECT
        inner_q.*,
        (watt_per_hour / 1000) * (inner_q.price_per_kwh / 100) AS costs_per_hour
      FROM (
        SELECT
          MAX(current_power) AS max_power,
          SUM(ws_dot_seconds) / 3600 AS watt_per_hour,
          BOOL_OR(power_on) AS power_on,
          date,
          date_trunc('hour', time) AS hour,
          MAX(price_per_kwh) AS price_per_kwh
        FROM mystrom_coffee_usage
        GROUP BY date, date_trunc('hour', time)
        ORDER BY date DESC, hour DESC
      ) as inner_q;
    """,
  )

  create_mystrom_coffee_view_usage_daily = SQLExecuteQueryOperator(
    task_id="create_mystrom_coffee_view_usage_daily",
    conn_id="airflow_data",
    sql="""
      CREATE OR REPLACE VIEW mystrom_coffee_usage_daily AS
      SELECT
        MAX(max_power) AS max_power,
        SUM(watt_per_hour) AS watt_per_day,
        SUM(costs_per_hour) AS costs_per_day,
        BOOL_OR(power_on) AS power_on,
        date
      FROM mystrom_coffee_usage_hourly
      GROUP BY date
      ORDER BY date DESC;
    """,
  )

  create_mystrom_coffee_table >> create_mystrom_coffee_view_usage >> create_mystrom_coffee_view_usage_hourly >> create_mystrom_coffee_view_usage_daily

dag = MyStromCoffeeInitDB()