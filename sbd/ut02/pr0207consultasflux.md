```python
import os
import csv
import influxdb_client
```


```python
# - - - CLIENTE - - -
INFLUX_URL = "http://influxdb-influxdb2-1:8086"
INFLUX_TOKEN = "Villabalter1"
INFLUX_ORG = "docs"

cliente = None
write_api = None

try:
    cliente = influxdb_client.InfluxDBClient(
        url = INFLUX_URL,
        token = INFLUX_TOKEN,
        org = INFLUX_ORG
    )

except (InfluxDBError, NewConnectionError) as e:
    print("[ERROR] Error al conectar con InfluxDB")
```


```python
# 1.1
query = """
from(bucket: "crypto_raw")
|> range(
    start: time(v: "2020-12-01T00:00:00Z"),
    stop: time(v: "2021-01-01T00:00:00Z")
    )
|> filter(fn: (r) => r.Symbol == "BTC")
|> filter(fn: (r) => r._field == "Close")
"""

query_api = cliente.query_api()
result = query_api.query(org=INFLUX_ORG, query=query)

for table in result:
    for record in table.records:
        print(
            record.get_time(),
            record.get_value()
        )
```


```python
# 1.2
query = """
from(bucket: "crypto_raw")
  |> range(
      start: time(v: "2021-01-01T00:00:00Z"),
      stop:  time(v: "2021-07-01T00:00:00Z")
  )
  |> filter(fn: (r) => r.Symbol == "ETHUSDT")
  |> filter(fn: (r) => r._field == "Volume")
  |> sum()
"""

query_api = cliente.query_api()
result = query_api.query(org=INFLUX_ORG, query=query)

for table in result:
    for record in table.records:
        print("Volumen total ETH (ene–jun 2021):", record.get_value())

```


```python
# 2.1
query = """
from(bucket: "crypto_raw")
  |> range(start: 0)
  |> filter(fn: (r) => r.Symbol == "BTC")
  |> filter(fn: (r) => r._field == "Close")
  |> aggregateWindow(
      every: 1mo,
      fn: mean,
      createEmpty: false
  )
"""

query_api = cliente.query_api()
result = query_api.query(org=INFLUX_ORG, query=query)

for table in result:
    for record in table.records:
        print(
            record.get_time(),
            record.get_value()
        )
```


```python
# 2.2
query = """
from(bucket: "crypto_raw")
  |> range(
      start: time(v: "2019-01-01T00:00:00Z"),
      stop:  time(v: "2020-01-01T00:00:00Z")
  )
  |> filter(fn: (r) => r.Symbol == "XLM")
  |> filter(fn: (r) => r._field == "Close")
  |> aggregateWindow(
      every: 1w,
      fn: max,
      createEmpty: false
  )
  |> yield(name: "max_weekly")

from(bucket: "crypto_raw")
  |> range(
      start: time(v: "2019-01-01T00:00:00Z"),
      stop:  time(v: "2020-01-01T00:00:00Z")
  )
  |> filter(fn: (r) => r.Symbol == "XLM")
  |> filter(fn: (r) => r._field == "Close")
  |> aggregateWindow(
      every: 1w,
      fn: min,
      createEmpty: false
  )
  |> yield(name: "min_weekly")
"""

query_api = cliente.query_api()
result = query_api.query(org=INFLUX_ORG, query=query)

for table in result:
    print("Resultado:", table.records[0].get_measurement())
    for record in table.records[:5]:
        print(record.get_time(), record.get_value())
    print("-----")
```


```python
# 3.1
query = """
from(bucket: "crypto_raw")
  |> range(
      start: time(v: "2016-01-01T00:00:00Z"),
      stop:  time(v: "2019-01-01T00:00:00Z")
  )
  |> filter(fn: (r) => r.Symbol == "USDT")
  |> filter(fn: (r) => r._field == "Close")
  |> sort(columns: ["_time"])
  |> difference(
      columns: ["_value"],
      keepFirst: true
  )
  |> map(fn: (r) => ({
      r with
      daily_pct_change: (r._value / r._value_previous) * 100.0
  }))
"""

query_api = cliente.query_api()
result = query_api.query(org=INFLUX_ORG, query=query)

for table in result:
    for record in table.records[:5]:
        print(
            record.get_time(),
            record.values["daily_pct_change"]
        )
```


```python
# 3.2
query = """
btc_data =
from(bucket: "crypto_raw")
  |> range(
      start: time(v: "2020-01-01T00:00:00Z"),
      stop:  time(v: "2021-01-01T00:00:00Z")
  )
  |> filter(fn: (r) => r.Symbol == "BTC")
  |> filter(fn: (r) => r._field == "Close")
  |> keep(columns: ["_time", "_value"])

eth_data =
from(bucket: "crypto_raw")
  |> range(
      start: time(v: "2020-01-01T00:00:00Z"),
      stop:  time(v: "2021-01-01T00:00:00Z")
  )
  |> filter(fn: (r) => r.Symbol == "ETH")
  |> filter(fn: (r) => r._field == "Close")
  |> keep(columns: ["_time", "_value"])

join(
  tables: {btc_data: btc_data, eth_data: eth_data},
  on: ["_time"]
)
|> map(fn: (r) => ({
    _time: r._time,
    btc_close: r.btc_data._value,
    eth_close: r.eth_data._value,
    ratio: r.btc_data._value / r.eth_data._value
}))
"""

query_api = cliente.query_api()
result = query_api.query(org=INFLUX_ORG, query=query)

for table in result:
    for record in table.records[:5]:
        print(
            record.get_time(),
            record.values["btc_close"],
            record.values["eth_close"],
            record.values["ratio"]
        )
```
