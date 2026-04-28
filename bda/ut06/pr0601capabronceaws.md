```python
import pandas as pd
import boto3
import requests
import json
import time
from datetime import datetime
```


```python
try:
    s3 = boto3.client("s3")
    buckets = s3.list_buckets()
    print("Conexión establecida!")
except Exception as e:
    print("Error de conexión!")
    print(e)
```

    Conexión establecida!



```python
df = pd.read_csv("Playas_españolas.csv")

df = df[["Nombre", "Provincia", "Término_M", "Duchas", "Aseos", "Acceso_dis", "Bandera_az", "Longitud", "X", "Y", "Grado_ocup", "Grado_urba"]]

df.to_csv("playas.csv", index=False)
```


```python
nombre_bucket = "jesus-riestra-ricote-capa-bronce"
key = "bronce/catalogos/guia_playas/v1/playas.csv"

s3.upload_file(
    "playas.csv",
    nombre_bucket,
    key
)

print("CSV subido correctamente a S3")
```

    CSV subido correctamente a S3



```python
API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqcmllc3RyYTA0QGdtYWlsLmNvbSIsImp0aSI6ImZmZDc3MzE0LTcwNWUtNDlkOC04ZTRkLWNmZjVmYWRjM2Q2MyIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzc2MjQ5NDU4LCJ1c2VySWQiOiJmZmQ3NzMxNC03MDVlLTQ5ZDgtOGU0ZC1jZmY1ZmFkYzNkNjMiLCJyb2xlIjoiIn0.ZwQz2MQ13v8OQ3407QM51MY3UtC49lPVBbrpIuNM2oA"
BUCKET = "jesus-riestra-ricote-capa-bronce"

headers = {"User-Agent": "Mozilla/5.0"}

municipios = set(df["Término_M"].dropna().unique())

codigos_municipios = {
    "Marbella": "29069",
    "Valencia": "46250",
    "Barcelona": "08019",
    "Málaga": "29067",
    "Alicante": "03014"
}

resultados = {}

for municipio in municipios:

    if municipio not in codigos_municipios:
        print(f"Sin código INE: {municipio}")
        continue

    codigo = codigos_municipios[municipio]

    url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/{codigo}?api_key={API_KEY}"

    try:
        r = requests.get(url, headers=headers, timeout=30)

        if r.status_code != 200:
            print(f"{municipio}: error HTTP {r.status_code}")
            continue

        data = r.json()

        if data.get("estado") != 200:
            print(f"{municipio}: {data.get('descripcion')}")
            continue

        datos_url = data["datos"]

        time.sleep(1)

        r2 = requests.get(datos_url, headers=headers, timeout=30)

        if r2.status_code != 200:
            print(f"{municipio}: error descarga datos")
            continue

        prediccion = r2.json()

        resultados[municipio] = prediccion

        print(f"OK: {municipio}")

    except Exception as e:
        print(f"Error en {municipio}: {e}")

fecha = datetime.now()

key = f"bronce/meteorologia/prediccion_playas/year={fecha.year}/month={fecha.month:02d}/day={fecha.day:02d}/prediccion.json"

s3.put_object(
    Bucket=BUCKET,
    Key=key,
    Body=json.dumps(resultados, ensure_ascii=False),
    ContentType="application/json"
)

print("Ingesta completada en S3")
```
