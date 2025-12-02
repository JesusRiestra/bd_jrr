```python
%%writefile agente_monitoreo.py
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client import WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
from urllib3.exceptions import NewConnectionError
from influxdb_client import Point

import psutil
import time

# Obtener estadísticas de uso
def obtener_metricas_sistema(host_id):
    # Uso de CPU (promedio de los últimos segundos)
    cpu_usage = psutil.cpu_percent(interval=1)
    
    # Uso de RAM
    mem = psutil.virtual_memory()
    ram_used_gb = round(mem.used / (1024**3), 2) # Conversión a GB
    ram_percent = mem.percent
    
    # Uso de disco (en el punto de montaje raíz)
    disk = psutil.disk_usage('/')
    disk_percent = disk.percent
    
    return {
        'host': host_id,
        'cpu_percent': cpu_usage,
        'ram_used_gb': ram_used_gb,
        'ram_percent': ram_percent,
        'disk_percent': disk_percent
    }

def agente_monitoreo():
    # - - - CLIENTE - - -
    INFLUX_URL = "http://influxdb2:8086"
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


    # - - - WRITEAPI - - -
    write_options = WriteOptions(
        batch_size = 500,
        flush_interval = 1000,
        write_type = ASYNCHRONOUS 
    )
    
    write_api = cliente.write_api(write_options = write_options)


    # - - - LECTURA - - -
    try:
        while True:
            metricas = obtener_metricas_sistema(host_id)

            punto = (
                Point("Rendimiento Servidor")            
                    .tag("host_id", metricas['host']) 
                    .tag("entorno", "produccion") 
                    .field("cpu_percent", metricas['cpu_percent']) 
                    .field("ram_percent", metricas['ram_percent']) 
                    .field("disk_percent", metricas['disk_percent'])
            )
        
            write_api.write(bucket="metricas", org="docs", record=punto)

            time.sleep(1)

    except KeyboardInput:
        print("Monitor detenido por el usuario [Ctrl + C]")

    finally:
        write_api.close()
        cliente.close()
        
```

    Overwriting agente_monitoreo.py

