```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```


```python
try:
    spark = ( SparkSession.builder
                  .appName("PR0501")
                  .master("spark://spark-master:7077")
                  .getOrCreate()
            )
    print("SparkSession iniciada correctamente!")

except Exception as e:
    print("Error en la conexión!")
    print(e)

sc = spark.sparkContext
```

    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    26/02/26 08:46:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


    SparkSession iniciada correctamente!



```python
# - - - ESQUEMA PRECIO CASAS - - -
esquema_casas = StructType([
    StructField("Index", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Amount (in rupees)", StringType(), True),
    StructField("Price (in rupees)", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("Carpet Area", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Floor", StringType(), True),
    StructField("Transaction", StringType(), True),
    StructField("Furnishing", StringType(), True),
    StructField("facing", StringType(), True),
    StructField("overlooking", StringType(), True),
    StructField("Society", StringType(), True),
    StructField("Bathroom", IntegerType(), True),
    StructField("Balcony", IntegerType(), True),
    StructField("Car Parking", StringType(), True),
    StructField("Ownership", StringType(), True),
    StructField("Super Area", StringType(), True),
    StructField("Dimensions", StringType(), True),
    StructField("Plot Area", StringType(), True),
])
```

    26/02/26 08:46:45 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors



```python
df_casas = ( spark.read
             .format("csv")
             .schema(esquema_casas)
             .option("header", "true")
             .option("quote", "\"")
             .load("house_prices.csv")
)
```


```python
df_casas = df_casas.select("Title", "Description", "Amount (in rupees)", "Price (in rupees)", "Carpet Area")
```


```python
df_casas.show()
```

                                                                                    

    +--------------------+--------------------+------------------+-----------------+-----------+
    |               Title|         Description|Amount (in rupees)|Price (in rupees)|Carpet Area|
    +--------------------+--------------------+------------------+-----------------+-----------+
    |1 BHK Ready to Oc...|Bhiwandi, Thane h...|           42 Lac |           6000.0|   500 sqft|
    |2 BHK Ready to Oc...|One can find this...|           98 Lac |          13799.0|   473 sqft|
    |2 BHK Ready to Oc...|Up for immediate ...|          1.40 Cr |          17500.0|   779 sqft|
    |1 BHK Ready to Oc...|This beautiful 1 ...|           25 Lac |             NULL|   530 sqft|
    |2 BHK Ready to Oc...|This lovely 2 BHK...|          1.60 Cr |          18824.0|   635 sqft|
    |1 BHK Ready to Oc...|Creatively planne...|           45 Lac |           6618.0|       NULL|
    |1 BHK Ready to Oc...|This magnificent ...|         16.5 Lac |           2538.0|   550 sqft|
    |1 BHK Ready to Oc...|Creatively planne...|           60 Lac |          10435.0|       NULL|
    |1 BHK Ready to Oc...|Discover this imm...|           60 Lac |          10000.0|       NULL|
    |3 BHK Ready to Oc...|One can find this...|          1.60 Cr |          11150.0|   900 sqft|
    |3 BHK Ready to Oc...|Up for immediate ...|          1.40 Cr |          12174.0|   950 sqft|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          1.36 Cr |          11674.0|       NULL|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          1.35 Cr |          15995.0|       NULL|
    |4 BHK Ready to Oc...|Creatively planne...|          4.25 Cr |          17526.0|  1820 sqft|
    |1 BHK Ready to Oc...|Discover this imm...|           75 Lac |          11538.0|       NULL|
    |2 BHK Ready to Oc...|Kasarvadavali, Th...|           90 Lac |          10000.0|   675 sqft|
    |1 BHK Ready to Oc...|Have a look at th...|           37 Lac |           5736.0|   647 sqft|
    |1 BHK Ready to Oc...|This magnificent ...|           35 Lac |           6481.0|       NULL|
    |2 BHK Ready to Oc...|Discover this imm...|           90 Lac |          11250.0|   600 sqft|
    |1 BHK Ready to Oc...|This lovely 1 BHK...|           35 Lac |           6731.0|       NULL|
    +--------------------+--------------------+------------------+-----------------+-----------+
    only showing top 20 rows
    



```python
# 1.1.1
df_casas = df_casas \
    .withColumn(
        "Unidad",
        (F.split(F.col("Amount (in rupees)"), " ")[1])
    ).withColumn(
        "Cantidad",
        (F.split(F.col("Amount (in rupees)"), " ")[0])
    )
```


```python
df_casas = df_casas \
    .withColumn(
        "price_value",
        F.when(F.col("Unidad") == "Lac", 100_000)
        .when(F.col("Unidad") == "Cr", 10_000_000)
    )
```


```python
df_casas = df_casas \
    .withColumn(
        "Cantidad",
        F.col("Cantidad").cast("decimal")
    )
```


```python
df_casas = df_casas \
    .withColumn(
        "Amount (in rupees)",
        F.round(F.col("Cantidad") * F.col("price_value"))
    )
```


```python
# 1.1.2
df_casas = df_casas \
    .withColumn(
        "Amount_USD",
        (F.col("Amount (in rupees)") * 0.012)
    )
```


```python
df_casas.show()
```

    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+
    |               Title|         Description|Amount (in rupees)|Price (in rupees)|Carpet Area|Unidad|Cantidad|price_value|Amount_USD|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+
    |1 BHK Ready to Oc...|Bhiwandi, Thane h...|           4200000|           6000.0|   500 sqft|   Lac|      42|     100000|   50400.0|
    |2 BHK Ready to Oc...|One can find this...|           9800000|          13799.0|   473 sqft|   Lac|      98|     100000|  117600.0|
    |2 BHK Ready to Oc...|Up for immediate ...|          10000000|          17500.0|   779 sqft|    Cr|       1|   10000000|  120000.0|
    |1 BHK Ready to Oc...|This beautiful 1 ...|           2500000|             NULL|   530 sqft|   Lac|      25|     100000|   30000.0|
    |2 BHK Ready to Oc...|This lovely 2 BHK...|          20000000|          18824.0|   635 sqft|    Cr|       2|   10000000|  240000.0|
    |1 BHK Ready to Oc...|Creatively planne...|           4500000|           6618.0|       NULL|   Lac|      45|     100000|   54000.0|
    |1 BHK Ready to Oc...|This magnificent ...|           1700000|           2538.0|   550 sqft|   Lac|      17|     100000|   20400.0|
    |1 BHK Ready to Oc...|Creatively planne...|           6000000|          10435.0|       NULL|   Lac|      60|     100000|   72000.0|
    |1 BHK Ready to Oc...|Discover this imm...|           6000000|          10000.0|       NULL|   Lac|      60|     100000|   72000.0|
    |3 BHK Ready to Oc...|One can find this...|          20000000|          11150.0|   900 sqft|    Cr|       2|   10000000|  240000.0|
    |3 BHK Ready to Oc...|Up for immediate ...|          10000000|          12174.0|   950 sqft|    Cr|       1|   10000000|  120000.0|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          11674.0|       NULL|    Cr|       1|   10000000|  120000.0|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          15995.0|       NULL|    Cr|       1|   10000000|  120000.0|
    |4 BHK Ready to Oc...|Creatively planne...|          40000000|          17526.0|  1820 sqft|    Cr|       4|   10000000|  480000.0|
    |1 BHK Ready to Oc...|Discover this imm...|           7500000|          11538.0|       NULL|   Lac|      75|     100000|   90000.0|
    |2 BHK Ready to Oc...|Kasarvadavali, Th...|           9000000|          10000.0|   675 sqft|   Lac|      90|     100000|  108000.0|
    |1 BHK Ready to Oc...|Have a look at th...|           3700000|           5736.0|   647 sqft|   Lac|      37|     100000|   44400.0|
    |1 BHK Ready to Oc...|This magnificent ...|           3500000|           6481.0|       NULL|   Lac|      35|     100000|   42000.0|
    |2 BHK Ready to Oc...|Discover this imm...|           9000000|          11250.0|   600 sqft|   Lac|      90|     100000|  108000.0|
    |1 BHK Ready to Oc...|This lovely 1 BHK...|           3500000|           6731.0|       NULL|   Lac|      35|     100000|   42000.0|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+
    only showing top 20 rows
    



```python
# 1.2
df_casas = df_casas \
    .withColumn(
        "Area_m2",
        F.round(F.split(F.col("Carpet Area"), " ")[0].cast("double") * 0.0929)
    )
```


```python
df_casas.show()
```

    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+
    |               Title|         Description|Amount (in rupees)|Price (in rupees)|Carpet Area|Unidad|Cantidad|price_value|Amount_USD|Area_m2|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+
    |1 BHK Ready to Oc...|Bhiwandi, Thane h...|           4200000|           6000.0|   500 sqft|   Lac|      42|     100000|   50400.0|   46.0|
    |2 BHK Ready to Oc...|One can find this...|           9800000|          13799.0|   473 sqft|   Lac|      98|     100000|  117600.0|   44.0|
    |2 BHK Ready to Oc...|Up for immediate ...|          10000000|          17500.0|   779 sqft|    Cr|       1|   10000000|  120000.0|   72.0|
    |1 BHK Ready to Oc...|This beautiful 1 ...|           2500000|             NULL|   530 sqft|   Lac|      25|     100000|   30000.0|   49.0|
    |2 BHK Ready to Oc...|This lovely 2 BHK...|          20000000|          18824.0|   635 sqft|    Cr|       2|   10000000|  240000.0|   59.0|
    |1 BHK Ready to Oc...|Creatively planne...|           4500000|           6618.0|       NULL|   Lac|      45|     100000|   54000.0|   NULL|
    |1 BHK Ready to Oc...|This magnificent ...|           1700000|           2538.0|   550 sqft|   Lac|      17|     100000|   20400.0|   51.0|
    |1 BHK Ready to Oc...|Creatively planne...|           6000000|          10435.0|       NULL|   Lac|      60|     100000|   72000.0|   NULL|
    |1 BHK Ready to Oc...|Discover this imm...|           6000000|          10000.0|       NULL|   Lac|      60|     100000|   72000.0|   NULL|
    |3 BHK Ready to Oc...|One can find this...|          20000000|          11150.0|   900 sqft|    Cr|       2|   10000000|  240000.0|   84.0|
    |3 BHK Ready to Oc...|Up for immediate ...|          10000000|          12174.0|   950 sqft|    Cr|       1|   10000000|  120000.0|   88.0|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          11674.0|       NULL|    Cr|       1|   10000000|  120000.0|   NULL|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          15995.0|       NULL|    Cr|       1|   10000000|  120000.0|   NULL|
    |4 BHK Ready to Oc...|Creatively planne...|          40000000|          17526.0|  1820 sqft|    Cr|       4|   10000000|  480000.0|  169.0|
    |1 BHK Ready to Oc...|Discover this imm...|           7500000|          11538.0|       NULL|   Lac|      75|     100000|   90000.0|   NULL|
    |2 BHK Ready to Oc...|Kasarvadavali, Th...|           9000000|          10000.0|   675 sqft|   Lac|      90|     100000|  108000.0|   63.0|
    |1 BHK Ready to Oc...|Have a look at th...|           3700000|           5736.0|   647 sqft|   Lac|      37|     100000|   44400.0|   60.0|
    |1 BHK Ready to Oc...|This magnificent ...|           3500000|           6481.0|       NULL|   Lac|      35|     100000|   42000.0|   NULL|
    |2 BHK Ready to Oc...|Discover this imm...|           9000000|          11250.0|   600 sqft|   Lac|      90|     100000|  108000.0|   56.0|
    |1 BHK Ready to Oc...|This lovely 1 BHK...|           3500000|           6731.0|       NULL|   Lac|      35|     100000|   42000.0|   NULL|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+
    only showing top 20 rows
    



```python
# 2.1
df_casas.agg(
    F.mean("Amount_USD").alias("Media"),
    F.variance("Amount_USD").alias("Varianza"),
    F.stddev("Amount_USD").alias("Desviacion Estandar")
).show()


# PREGUNTA:
# Si la desviación estándar es muy alta en comparación con el precio medio (por ejemplo, si la media es $100k y la desviación es $80k), 
# ¿podemos decir que el “precio promedio” es un buen representante del mercado? ¿O los precios son demasiado dispares para confiar en el promedio?

# -En este caso la media no es un buen representante del mercado, puesto que los precios son demasiado dispares.

```

    [Stage 3:=======>                                                   (1 + 7) / 8]

    +------------------+--------------------+-------------------+
    |             Media|            Varianza|Desviacion Estandar|
    +------------------+--------------------+-------------------+
    |143681.62163681857|2.244137688139068E11| 473723.30406462675|
    +------------------+--------------------+-------------------+
    


                                                                                    


```python
# 2.2
df_casas.agg(
    F.skewness("Amount_USD").alias("Asimetría"),
    F.kurtosis("Amount_USD").alias("Curtosis")
).show()

# PREGUNTA:
# Suponiendo que has obtenido un valor positivo, 
# ¿qué significa esto para el negocio? ¿Hay más oferta de casas “baratas” con algunas pocas mansiones ultra-caras, o hay muchas casas caras y pocas baratas?

# -Hay una cantidad exageradamente alta de casas baratas en comparación con las casas caras.


# PREGUNTA
# Supón que obtienes una kurtosis superior a 3. 
# ¿Deberíamos preocuparnos por la presencia de datos erróneos o propiedades de lujo extremo que podrían distorsionar nuestros análisis futuros?

# - Si, es casi seguro que haya outliers muy importantes y/o propiedades ultra exclusivas completamente fuera del mercado promedio.
```

    [Stage 6:======================>                                    (3 + 5) / 8]

    +-----------------+----------------+
    |        Asimetría|        Curtosis|
    +-----------------+----------------+
    |269.2983424955631|90820.2137193298|
    +-----------------+----------------+
    


                                                                                    


```python
# 3.1
estadisticas = df_casas.agg(
    F.mean("Amount_USD").alias("Media Amount_USD"),
    F.stddev("Amount_USD").alias("Desv. Estandar Amount_USD"),
    F.mean("Area_m2").alias("Media Area_m2"),
    F.stddev("Area_m2").alias("Desv. Estandar Area_m2")
)

estadisticas.show()

valores = estadisticas.collect()[0]

media_precio = valores["Media Amount_USD"]
desviacion_precio = valores["Desv. Estandar Amount_USD"]
media_area = valores["Media Area_m2"]
desviacion_area = valores["Desv. Estandar Area_m2"]

df_normalizado = df_casas.withColumn(
    "Amount_USD_Normalizado",
    (F.col("Amount_USD") - media_precio) / desviacion_precio
).withColumn(
    "Area_m2_Normalizado",
    (F.col("Area_m2") - media_area) / desviacion_area
)

df_normalizado.show()
```

                                                                                    

    +------------------+-------------------------+------------------+----------------------+
    |  Media Amount_USD|Desv. Estandar Amount_USD|     Media Area_m2|Desv. Estandar Area_m2|
    +------------------+-------------------------+------------------+----------------------+
    |143681.62163681857|       473723.30406462675|111.51801907186105|     283.1057091581172|
    +------------------+-------------------------+------------------+----------------------+
    


                                                                                    

    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+----------------------+--------------------+
    |               Title|         Description|Amount (in rupees)|Price (in rupees)|Carpet Area|Unidad|Cantidad|price_value|Amount_USD|Area_m2|Amount_USD_Normalizado| Area_m2_Normalizado|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+----------------------+--------------------+
    |1 BHK Ready to Oc...|Bhiwandi, Thane h...|           4200000|           6000.0|   500 sqft|   Lac|      42|     100000|   50400.0|   46.0|  -0.19691161662609027|-0.23142599019530413|
    |2 BHK Ready to Oc...|One can find this...|           9800000|          13799.0|   473 sqft|   Lac|      98|     100000|  117600.0|   44.0|  -0.05505665736313542|-0.23849048919798224|
    |2 BHK Ready to Oc...|Up for immediate ...|          10000000|          17500.0|   779 sqft|    Cr|       1|   10000000|  120000.0|   72.0|  -0.04999040881802989| -0.1395875031604886|
    |1 BHK Ready to Oc...|This beautiful 1 ...|           2500000|             NULL|   530 sqft|   Lac|      25|     100000|   30000.0|   49.0|  -0.23997472925948726|-0.22082924169128695|
    |2 BHK Ready to Oc...|This lovely 2 BHK...|          20000000|          18824.0|   635 sqft|    Cr|       2|   10000000|  240000.0|   59.0|   0.20332201843724662|-0.18550674667789635|
    |1 BHK Ready to Oc...|Creatively planne...|           4500000|           6618.0|       NULL|   Lac|      45|     100000|   54000.0|   NULL|  -0.18931224380843198|                NULL|
    |1 BHK Ready to Oc...|This magnificent ...|           1700000|           2538.0|   550 sqft|   Lac|      17|     100000|   20400.0|   51.0|   -0.2602397234399094|-0.21376474268860884|
    |1 BHK Ready to Oc...|Creatively planne...|           6000000|          10435.0|       NULL|   Lac|      60|     100000|   72000.0|   NULL|   -0.1513153797201405|                NULL|
    |1 BHK Ready to Oc...|Discover this imm...|           6000000|          10000.0|       NULL|   Lac|      60|     100000|   72000.0|   NULL|   -0.1513153797201405|                NULL|
    |3 BHK Ready to Oc...|One can find this...|          20000000|          11150.0|   900 sqft|    Cr|       2|   10000000|  240000.0|   84.0|   0.20332201843724662| -0.0972005091444199|
    |3 BHK Ready to Oc...|Up for immediate ...|          10000000|          12174.0|   950 sqft|    Cr|       1|   10000000|  120000.0|   88.0|  -0.04999040881802989|-0.08307151113906366|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          11674.0|       NULL|    Cr|       1|   10000000|  120000.0|   NULL|  -0.04999040881802989|                NULL|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          15995.0|       NULL|    Cr|       1|   10000000|  120000.0|   NULL|  -0.04999040881802989|                NULL|
    |4 BHK Ready to Oc...|Creatively planne...|          40000000|          17526.0|  1820 sqft|    Cr|       4|   10000000|  480000.0|  169.0|    0.7099468729477997|  0.2030406984694001|
    |1 BHK Ready to Oc...|Discover this imm...|           7500000|          11538.0|       NULL|   Lac|      75|     100000|   90000.0|   NULL|  -0.11331851563184901|                NULL|
    |2 BHK Ready to Oc...|Kasarvadavali, Th...|           9000000|          10000.0|   675 sqft|   Lac|      90|     100000|  108000.0|   63.0|  -0.07532165154355754|-0.17137774867254013|
    |1 BHK Ready to Oc...|Have a look at th...|           3700000|           5736.0|   647 sqft|   Lac|      37|     100000|   44400.0|   60.0|  -0.20957723798885408| -0.1819744971765573|
    |1 BHK Ready to Oc...|This magnificent ...|           3500000|           6481.0|       NULL|   Lac|      35|     100000|   42000.0|   NULL|  -0.21464348653395962|                NULL|
    |2 BHK Ready to Oc...|Discover this imm...|           9000000|          11250.0|   600 sqft|   Lac|      90|     100000|  108000.0|   56.0|  -0.07532165154355754|-0.19610349518191353|
    |1 BHK Ready to Oc...|This lovely 1 BHK...|           3500000|           6731.0|       NULL|   Lac|      35|     100000|   42000.0|   NULL|  -0.21464348653395962|                NULL|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+----------------------+--------------------+
    only showing top 20 rows
    



```python
# 3.2
percentil_99 = df_casas.approxQuantile("Amount_USD", [0.99], 0.01)[0]
print("Percentil 99 (limite superior):", percentil_99)

df_limpio = df_casas.filter(F.col("Amount_USD") <= percentil_99)
print("Total original:", df_casas.count())
print("Total después del filtro:", df_limpio.count())

metricas_limpio = df_limpio.agg(
    F.mean("Amount_USD").alias("Media"),
    F.stddev("Amount_USD").alias("Desviación"),
    F.kurtosis("Amount_USD").alias("Curtosis")
)
metricas_limpio.show()

# PREGUNTA
# Comparando los resultados antes y después del filtro: 
# ¿Cuánto ha bajado la curtosis? ¿Ha cambiado drásticamente el precio medio al eliminar solo el 1% de los datos? 
# Reflexiona sobre la sensibilidad de la media aritmética frente a los outliers en grandes volúmenes de datos.

# - Ninguna de las métricas ha cambiado. Esto puede deberse a que hay muchos valores extremedamente grandes, 
#   los cuales no están concentrados únicamente en el 1% superior,por lo que eliminar solo el 1% no es suficiente para reducir la curtosis.
```

                                                                                    

    Percentil 99 (limite superior): 168000000.0


                                                                                    

    Total original: 187531


                                                                                    

    Total después del filtro: 177845


    [Stage 115:=======>                                                 (1 + 7) / 8]

    +------------------+------------------+----------------+
    |             Media|        Desviación|        Curtosis|
    +------------------+------------------+----------------+
    |143681.62163681857|473723.30406462675|90820.2137193298|
    +------------------+------------------+----------------+
    


                                                                                    


```python
# 4.1
df_casas = df_casas \
    .withColumn(
        "Num_Bedrooms",
        F.split(F.col("Title"), " ")[0].cast("int")
    )

df_casas.show()
```

    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+------------+
    |               Title|         Description|Amount (in rupees)|Price (in rupees)|Carpet Area|Unidad|Cantidad|price_value|Amount_USD|Area_m2|Num_Bedrooms|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+------------+
    |1 BHK Ready to Oc...|Bhiwandi, Thane h...|           4200000|           6000.0|   500 sqft|   Lac|      42|     100000|   50400.0|   46.0|           1|
    |2 BHK Ready to Oc...|One can find this...|           9800000|          13799.0|   473 sqft|   Lac|      98|     100000|  117600.0|   44.0|           2|
    |2 BHK Ready to Oc...|Up for immediate ...|          10000000|          17500.0|   779 sqft|    Cr|       1|   10000000|  120000.0|   72.0|           2|
    |1 BHK Ready to Oc...|This beautiful 1 ...|           2500000|             NULL|   530 sqft|   Lac|      25|     100000|   30000.0|   49.0|           1|
    |2 BHK Ready to Oc...|This lovely 2 BHK...|          20000000|          18824.0|   635 sqft|    Cr|       2|   10000000|  240000.0|   59.0|           2|
    |1 BHK Ready to Oc...|Creatively planne...|           4500000|           6618.0|       NULL|   Lac|      45|     100000|   54000.0|   NULL|           1|
    |1 BHK Ready to Oc...|This magnificent ...|           1700000|           2538.0|   550 sqft|   Lac|      17|     100000|   20400.0|   51.0|           1|
    |1 BHK Ready to Oc...|Creatively planne...|           6000000|          10435.0|       NULL|   Lac|      60|     100000|   72000.0|   NULL|           1|
    |1 BHK Ready to Oc...|Discover this imm...|           6000000|          10000.0|       NULL|   Lac|      60|     100000|   72000.0|   NULL|           1|
    |3 BHK Ready to Oc...|One can find this...|          20000000|          11150.0|   900 sqft|    Cr|       2|   10000000|  240000.0|   84.0|           3|
    |3 BHK Ready to Oc...|Up for immediate ...|          10000000|          12174.0|   950 sqft|    Cr|       1|   10000000|  120000.0|   88.0|           3|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          11674.0|       NULL|    Cr|       1|   10000000|  120000.0|   NULL|           2|
    |2 BHK Ready to Oc...|2 BHK, Multistore...|          10000000|          15995.0|       NULL|    Cr|       1|   10000000|  120000.0|   NULL|           2|
    |4 BHK Ready to Oc...|Creatively planne...|          40000000|          17526.0|  1820 sqft|    Cr|       4|   10000000|  480000.0|  169.0|           4|
    |1 BHK Ready to Oc...|Discover this imm...|           7500000|          11538.0|       NULL|   Lac|      75|     100000|   90000.0|   NULL|           1|
    |2 BHK Ready to Oc...|Kasarvadavali, Th...|           9000000|          10000.0|   675 sqft|   Lac|      90|     100000|  108000.0|   63.0|           2|
    |1 BHK Ready to Oc...|Have a look at th...|           3700000|           5736.0|   647 sqft|   Lac|      37|     100000|   44400.0|   60.0|           1|
    |1 BHK Ready to Oc...|This magnificent ...|           3500000|           6481.0|       NULL|   Lac|      35|     100000|   42000.0|   NULL|           1|
    |2 BHK Ready to Oc...|Discover this imm...|           9000000|          11250.0|   600 sqft|   Lac|      90|     100000|  108000.0|   56.0|           2|
    |1 BHK Ready to Oc...|This lovely 1 BHK...|           3500000|           6731.0|       NULL|   Lac|      35|     100000|   42000.0|   NULL|           1|
    +--------------------+--------------------+------------------+-----------------+-----------+------+--------+-----------+----------+-------+------------+
    only showing top 20 rows
    



```python
# 4.2
df_agrupado = df_casas.groupBy("Num_Bedrooms").agg(
    F.mean("Amount_USD").alias("Media_Precio"),
    F.stddev("Amount_USD").alias("Desviación_Precio"),
    F.skewness("Amount_USD").alias("Skewness_Precio")
)

df_agrupado.orderBy("Num_Bedrooms").show()
```

    [Stage 119:=====================>                                   (3 + 5) / 8]

    +------------+------------------+------------------+--------------------+
    |Num_Bedrooms|      Media_Precio| Desviación_Precio|     Skewness_Precio|
    +------------+------------------+------------------+--------------------+
    |        NULL|174268.19484240687| 685899.9401259893|   9.376823475033591|
    |           1| 42289.76319224315| 32464.03120025952|   4.651674878180098|
    |           2|  74750.1447301209|213409.69915946128|   187.6387867186499|
    |           3|164310.54110050463| 647535.0148779893|  231.99199324253487|
    |           4|370650.50505050505|298056.51740132354|   4.019988436809687|
    |           5| 565208.4284460052|473137.58648634347|   6.182217870681146|
    |           6| 852769.5652173914| 910458.0182631097|   1.808691160466272|
    |           7| 693085.7142857143| 728066.2732785486|   1.799795191952248|
    |           8|         2528925.0|1934189.4659003806|0.054927816752066996|
    |           9|445714.28571428574|215936.49860218496| -0.2837292768901801|
    |          10| 800290.9090909091|  1361820.30052026|  2.6018034165462063|
    +------------+------------------+------------------+--------------------+
    


                                                                                    


```python
# 4.3

# A. ANÁLISIS DE VARIABILIDAD (DESVIACIÓN ESTÁNDAR):
# Si la desviación es mucho mayor en los pisos de 3 BHK que en los de 1 BHK, ¿qué nos indica esto sobre la homogeneidad del producto?

# - El mercado de 1 BHK es homogéneo, mientras que el mercado de 3 BHK es altamente heterogéneo, conviviendo pisos familiares estándar con propiedades de lujo.



# B. CONFIABILIDAD DEL PRECIO PROMEDIO:
# Basándote en lo anterior, ¿en qué segmento (1 BHK o 3 BHK) dirías que el “Precio Promedio” es un indicador más fiable del valor real de una propiedad? 
# Es decir, si tuvieras que tasar una propiedad “a ciegas” usando solo el promedio del mercado, ¿en qué tipo de apartamento tendrías más riesgo de equivocarte drásticamente por exceso o por defecto?

# - El promedio en el segmento 1 BHK es mucho más fiable, ya que presenta menor dispersión, y sus valores son más homogéneos.



# C. DETECCIÓN DE ANOMALÍAS DE MERCADO (CURTOSIS):
# ¿Qué segmento tiene una curtosis más alta (colas más pesadas)?

# - El segmento 3 BHK tiene una curtosis más alta, con colas muy pesadas y presencia de propiedades ultra caras.


# Si el segmento de 3 BHK tiene una curtosis muy elevada, significa que existen propiedades con precios desorbitados que rompen la norma. 
# ¿Consideras que estas “mansiones” representan la realidad del barrio, o son excepciones que deberían analizarse en un estudio de mercado aparte para no distorsionar la visión general?

# - Las mansiones no reflejan la realidad típica del barrio, sino que son excepciones que deberían analizarse por separado para evitar que distorsionen la visión general de los precios promedio.
```
