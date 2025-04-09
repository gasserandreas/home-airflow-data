# mystrom DAGs

## Requirements

- `airflow_data` Postgres connection
- `mystrom_coffee_dag_var` variable containing:
```
{
  "ip_address": "ip-address-of-mystrom-device",
  "power_price_intervals": [
    {
      "start": "00:00",
      "end": "07:00",
      "price": 26.14
    },
    {
      "start": "07:00",
      "end": "20:00",
      "price": 31.54
    },
    {
      "start": "20:00",
      "end": "24:00",
      "price": 26.14
    }
  ]
}
```

## Installation

1. make sure dependencies are ready
2. run `mystrom_coffee_init_db`
3. enable other DAGs