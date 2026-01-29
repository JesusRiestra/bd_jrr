```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, lit, lpad, split, when, lower, upper, log, round, bround, greatest, to_date, date_add, month
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


```python
# - - - DATASET 1 - - -
esquema_cultivos = StructType([
    StructField("Crop", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Soil_Type", StringType(), True),
    StructField("Soil_pH", DoubleType(), True),
    StructField("Rainfall_mm", DoubleType(), True),
    StructField("Temperature_C", DoubleType(), True),
    StructField("Humidity_pct", DoubleType(), True),
    StructField("Fertilizer_Used_kg", DoubleType(), True),
    StructField("Irrigation", StringType(), True),
    StructField("Pesticides_Used_kg", DoubleType(), True),
    StructField("Planting_Density", DoubleType(), True),
    StructField("Previous_Crop", StringType(), True),
    StructField("Yield_ton_per_ha", DoubleType(), True)
])
```


```python
df_cultivos = ( spark.read
                    .format("csv")
                    .schema(esquema_cultivos)
                    .option("header", "true")
                    .option("quote", "\"")
                    .load("./crop_yield_dataset.csv")
                )
```


```python
df_cultivos.show(10)
```

    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+
    |  Crop|  Region|Soil_Type|Soil_pH|Rainfall_mm|Temperature_C|Humidity_pct|Fertilizer_Used_kg|Irrigation|Pesticides_Used_kg|Planting_Density|Previous_Crop|Yield_ton_per_ha|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+
    | Maize|Region_C|    Sandy|   7.01|     1485.4|         19.7|        40.3|             105.1|      Drip|              10.2|            23.2|         Rice|          101.48|
    |Barley|Region_D|     Loam|   5.79|      399.4|         29.1|        55.4|             221.8| Sprinkler|              35.5|             7.4|       Barley|          127.39|
    |  Rice|Region_C|     Clay|   7.24|      980.9|         30.5|        74.4|              61.2| Sprinkler|              40.0|             5.1|        Wheat|           68.99|
    | Maize|Region_D|     Loam|   6.79|     1054.3|         26.4|        62.0|             257.8|      Drip|              42.7|            23.7|         None|          169.06|
    | Maize|Region_D|    Sandy|   5.96|      744.6|         20.4|        70.9|             195.8|      Drip|              25.5|            15.6|        Maize|          118.71|
    |Barley|Region_C|    Sandy|   5.82|      817.5|         23.1|        47.6|              64.6|      None|              16.4|            16.2|        Maize|           58.85|
    |  Rice|Region_B|    Sandy|   6.76|     1358.2|         16.9|        31.9|             267.9| Sprinkler|              38.6|            23.6|         Rice|          173.44|
    |  Rice|Region_D|    Sandy|    7.3|     1038.9|         34.1|        31.7|             269.4| Sprinkler|              16.0|            19.0|       Barley|          170.05|
    | Maize|Region_C|     Loam|   6.94|      846.1|         32.4|        86.6|             263.2|      None|               7.4|            21.5|        Wheat|           162.2|
    | Wheat|Region_A|     Clay|    6.2|      366.9|         16.3|        86.7|             243.6| Sprinkler|              41.7|            20.4|        Wheat|          141.67|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+
    only showing top 10 rows
    



```python
# 1
df_eng = ( df_cultivos.withColumn(
            "Crop_ID",
            concat(
                lit("CODIGO_"),
                lpad(split(col("Region"), '_')[1], 3, 'X'),
                lit("-"),
                upper(col("Crop"))
            )
        )    
)
```


```python
df_eng.show(10)
```

    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+
    |  Crop|  Region|Soil_Type|Soil_pH|Rainfall_mm|Temperature_C|Humidity_pct|Fertilizer_Used_kg|Irrigation|Pesticides_Used_kg|Planting_Density|Previous_Crop|Yield_ton_per_ha|          Crop_ID|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+
    | Maize|Region_C|    Sandy|   7.01|     1485.4|         19.7|        40.3|             105.1|      Drip|              10.2|            23.2|         Rice|          101.48| CODIGO_XXC-MAIZE|
    |Barley|Region_D|     Loam|   5.79|      399.4|         29.1|        55.4|             221.8| Sprinkler|              35.5|             7.4|       Barley|          127.39|CODIGO_XXD-BARLEY|
    |  Rice|Region_C|     Clay|   7.24|      980.9|         30.5|        74.4|              61.2| Sprinkler|              40.0|             5.1|        Wheat|           68.99|  CODIGO_XXC-RICE|
    | Maize|Region_D|     Loam|   6.79|     1054.3|         26.4|        62.0|             257.8|      Drip|              42.7|            23.7|         None|          169.06| CODIGO_XXD-MAIZE|
    | Maize|Region_D|    Sandy|   5.96|      744.6|         20.4|        70.9|             195.8|      Drip|              25.5|            15.6|        Maize|          118.71| CODIGO_XXD-MAIZE|
    |Barley|Region_C|    Sandy|   5.82|      817.5|         23.1|        47.6|              64.6|      None|              16.4|            16.2|        Maize|           58.85|CODIGO_XXC-BARLEY|
    |  Rice|Region_B|    Sandy|   6.76|     1358.2|         16.9|        31.9|             267.9| Sprinkler|              38.6|            23.6|         Rice|          173.44|  CODIGO_XXB-RICE|
    |  Rice|Region_D|    Sandy|    7.3|     1038.9|         34.1|        31.7|             269.4| Sprinkler|              16.0|            19.0|       Barley|          170.05|  CODIGO_XXD-RICE|
    | Maize|Region_C|     Loam|   6.94|      846.1|         32.4|        86.6|             263.2|      None|               7.4|            21.5|        Wheat|           162.2| CODIGO_XXC-MAIZE|
    | Wheat|Region_A|     Clay|    6.2|      366.9|         16.3|        86.7|             243.6| Sprinkler|              41.7|            20.4|        Wheat|          141.67| CODIGO_XXA-WHEAT|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+
    only showing top 10 rows
    



```python
# 2
df_eng = ( df_eng.withColumn(
            "Log_Rainfall",
            log(col("Rainfall_mm") + 1)    
        ).withColumn(
            "Yield_Redondeado",
            round(col("Yield_ton_per_ha"), 1)
        ).withColumn(
            "Rendimiento_Sin_Decimales",
            bround(col("Yield_ton_per_ha"), 0)
        )
)
```


```python
df_eng.show(10)
```

    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+
    |  Crop|  Region|Soil_Type|Soil_pH|Rainfall_mm|Temperature_C|Humidity_pct|Fertilizer_Used_kg|Irrigation|Pesticides_Used_kg|Planting_Density|Previous_Crop|Yield_ton_per_ha|          Crop_ID|      Log_Rainfall|Yield_Redondeado|Rendimiento_Sin_Decimales|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+
    | Maize|Region_C|    Sandy|   7.01|     1485.4|         19.7|        40.3|             105.1|      Drip|              10.2|            23.2|         Rice|          101.48| CODIGO_XXC-MAIZE| 7.304112368059574|           101.5|                    101.0|
    |Barley|Region_D|     Loam|   5.79|      399.4|         29.1|        55.4|             221.8| Sprinkler|              35.5|             7.4|       Barley|          127.39|CODIGO_XXD-BARLEY| 5.992464047441065|           127.4|                    127.0|
    |  Rice|Region_C|     Clay|   7.24|      980.9|         30.5|        74.4|              61.2| Sprinkler|              40.0|             5.1|        Wheat|           68.99|  CODIGO_XXC-RICE| 6.889489470175245|            69.0|                     69.0|
    | Maize|Region_D|     Loam|   6.79|     1054.3|         26.4|        62.0|             257.8|      Drip|              42.7|            23.7|         None|          169.06| CODIGO_XXD-MAIZE| 6.961580365677045|           169.1|                    169.0|
    | Maize|Region_D|    Sandy|   5.96|      744.6|         20.4|        70.9|             195.8|      Drip|              25.5|            15.6|        Maize|          118.71| CODIGO_XXD-MAIZE| 6.614189263371381|           118.7|                    119.0|
    |Barley|Region_C|    Sandy|   5.82|      817.5|         23.1|        47.6|              64.6|      None|              16.4|            16.2|        Maize|           58.85|CODIGO_XXC-BARLEY|6.7074733968111895|            58.9|                     59.0|
    |  Rice|Region_B|    Sandy|   6.76|     1358.2|         16.9|        31.9|             267.9| Sprinkler|              38.6|            23.6|         Rice|          173.44|  CODIGO_XXB-RICE| 7.214651570357722|           173.4|                    173.0|
    |  Rice|Region_D|    Sandy|    7.3|     1038.9|         34.1|        31.7|             269.4| Sprinkler|              16.0|            19.0|       Barley|          170.05|  CODIGO_XXD-RICE| 6.946879833666187|           170.1|                    170.0|
    | Maize|Region_C|     Loam|   6.94|      846.1|         32.4|        86.6|             263.2|      None|               7.4|            21.5|        Wheat|           162.2| CODIGO_XXC-MAIZE| 6.741818751437505|           162.2|                    162.0|
    | Wheat|Region_A|     Clay|    6.2|      366.9|         16.3|        86.7|             243.6| Sprinkler|              41.7|            20.4|        Wheat|          141.67| CODIGO_XXA-WHEAT| 5.907811162110729|           141.7|                    142.0|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+
    only showing top 10 rows
    



```python
# 3
df_eng = ( df_eng.withColumn(
    "Max_Quimico_kg",
    greatest(
        col("Fertilizer_Used_kg"),
        col("Pesticides_Used_kg")
    )
    )
)
```


```python
df_eng.show(10)
```

    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+--------------+
    |  Crop|  Region|Soil_Type|Soil_pH|Rainfall_mm|Temperature_C|Humidity_pct|Fertilizer_Used_kg|Irrigation|Pesticides_Used_kg|Planting_Density|Previous_Crop|Yield_ton_per_ha|          Crop_ID|      Log_Rainfall|Yield_Redondeado|Rendimiento_Sin_Decimales|Max_Quimico_kg|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+--------------+
    | Maize|Region_C|    Sandy|   7.01|     1485.4|         19.7|        40.3|             105.1|      Drip|              10.2|            23.2|         Rice|          101.48| CODIGO_XXC-MAIZE| 7.304112368059574|           101.5|                    101.0|         105.1|
    |Barley|Region_D|     Loam|   5.79|      399.4|         29.1|        55.4|             221.8| Sprinkler|              35.5|             7.4|       Barley|          127.39|CODIGO_XXD-BARLEY| 5.992464047441065|           127.4|                    127.0|         221.8|
    |  Rice|Region_C|     Clay|   7.24|      980.9|         30.5|        74.4|              61.2| Sprinkler|              40.0|             5.1|        Wheat|           68.99|  CODIGO_XXC-RICE| 6.889489470175245|            69.0|                     69.0|          61.2|
    | Maize|Region_D|     Loam|   6.79|     1054.3|         26.4|        62.0|             257.8|      Drip|              42.7|            23.7|         None|          169.06| CODIGO_XXD-MAIZE| 6.961580365677045|           169.1|                    169.0|         257.8|
    | Maize|Region_D|    Sandy|   5.96|      744.6|         20.4|        70.9|             195.8|      Drip|              25.5|            15.6|        Maize|          118.71| CODIGO_XXD-MAIZE| 6.614189263371381|           118.7|                    119.0|         195.8|
    |Barley|Region_C|    Sandy|   5.82|      817.5|         23.1|        47.6|              64.6|      None|              16.4|            16.2|        Maize|           58.85|CODIGO_XXC-BARLEY|6.7074733968111895|            58.9|                     59.0|          64.6|
    |  Rice|Region_B|    Sandy|   6.76|     1358.2|         16.9|        31.9|             267.9| Sprinkler|              38.6|            23.6|         Rice|          173.44|  CODIGO_XXB-RICE| 7.214651570357722|           173.4|                    173.0|         267.9|
    |  Rice|Region_D|    Sandy|    7.3|     1038.9|         34.1|        31.7|             269.4| Sprinkler|              16.0|            19.0|       Barley|          170.05|  CODIGO_XXD-RICE| 6.946879833666187|           170.1|                    170.0|         269.4|
    | Maize|Region_C|     Loam|   6.94|      846.1|         32.4|        86.6|             263.2|      None|               7.4|            21.5|        Wheat|           162.2| CODIGO_XXC-MAIZE| 6.741818751437505|           162.2|                    162.0|         263.2|
    | Wheat|Region_A|     Clay|    6.2|      366.9|         16.3|        86.7|             243.6| Sprinkler|              41.7|            20.4|        Wheat|          141.67| CODIGO_XXA-WHEAT| 5.907811162110729|           141.7|                    142.0|         243.6|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+--------------+
    only showing top 10 rows
    



```python
# 4
df_eng = ( df_eng.withColumn(
            "Fecha_Siembra",
            to_date(lit("2023-04-01"))
        ).withColumn(
            "Fecha_Estimada_Cosecha",
            date_add(col("Fecha_Siembra"), 150)
        ).withColumn(
            "Mes_Cosecha",
            month(col("Fecha_Estimada_Cosecha"))
        )
)
```


```python
df_eng.show(10)
```

    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+--------------+-------------+----------------------+-----------+
    |  Crop|  Region|Soil_Type|Soil_pH|Rainfall_mm|Temperature_C|Humidity_pct|Fertilizer_Used_kg|Irrigation|Pesticides_Used_kg|Planting_Density|Previous_Crop|Yield_ton_per_ha|          Crop_ID|      Log_Rainfall|Yield_Redondeado|Rendimiento_Sin_Decimales|Max_Quimico_kg|Fecha_Siembra|Fecha_Estimada_Cosecha|Mes_Cosecha|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+--------------+-------------+----------------------+-----------+
    | Maize|Region_C|    Sandy|   7.01|     1485.4|         19.7|        40.3|             105.1|      Drip|              10.2|            23.2|         Rice|          101.48| CODIGO_XXC-MAIZE| 7.304112368059574|           101.5|                    101.0|         105.1|   2023-04-01|            2023-08-29|          8|
    |Barley|Region_D|     Loam|   5.79|      399.4|         29.1|        55.4|             221.8| Sprinkler|              35.5|             7.4|       Barley|          127.39|CODIGO_XXD-BARLEY| 5.992464047441065|           127.4|                    127.0|         221.8|   2023-04-01|            2023-08-29|          8|
    |  Rice|Region_C|     Clay|   7.24|      980.9|         30.5|        74.4|              61.2| Sprinkler|              40.0|             5.1|        Wheat|           68.99|  CODIGO_XXC-RICE| 6.889489470175245|            69.0|                     69.0|          61.2|   2023-04-01|            2023-08-29|          8|
    | Maize|Region_D|     Loam|   6.79|     1054.3|         26.4|        62.0|             257.8|      Drip|              42.7|            23.7|         None|          169.06| CODIGO_XXD-MAIZE| 6.961580365677045|           169.1|                    169.0|         257.8|   2023-04-01|            2023-08-29|          8|
    | Maize|Region_D|    Sandy|   5.96|      744.6|         20.4|        70.9|             195.8|      Drip|              25.5|            15.6|        Maize|          118.71| CODIGO_XXD-MAIZE| 6.614189263371381|           118.7|                    119.0|         195.8|   2023-04-01|            2023-08-29|          8|
    |Barley|Region_C|    Sandy|   5.82|      817.5|         23.1|        47.6|              64.6|      None|              16.4|            16.2|        Maize|           58.85|CODIGO_XXC-BARLEY|6.7074733968111895|            58.9|                     59.0|          64.6|   2023-04-01|            2023-08-29|          8|
    |  Rice|Region_B|    Sandy|   6.76|     1358.2|         16.9|        31.9|             267.9| Sprinkler|              38.6|            23.6|         Rice|          173.44|  CODIGO_XXB-RICE| 7.214651570357722|           173.4|                    173.0|         267.9|   2023-04-01|            2023-08-29|          8|
    |  Rice|Region_D|    Sandy|    7.3|     1038.9|         34.1|        31.7|             269.4| Sprinkler|              16.0|            19.0|       Barley|          170.05|  CODIGO_XXD-RICE| 6.946879833666187|           170.1|                    170.0|         269.4|   2023-04-01|            2023-08-29|          8|
    | Maize|Region_C|     Loam|   6.94|      846.1|         32.4|        86.6|             263.2|      None|               7.4|            21.5|        Wheat|           162.2| CODIGO_XXC-MAIZE| 6.741818751437505|           162.2|                    162.0|         263.2|   2023-04-01|            2023-08-29|          8|
    | Wheat|Region_A|     Clay|    6.2|      366.9|         16.3|        86.7|             243.6| Sprinkler|              41.7|            20.4|        Wheat|          141.67| CODIGO_XXA-WHEAT| 5.907811162110729|           141.7|                    142.0|         243.6|   2023-04-01|            2023-08-29|          8|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+-----------------+------------------+----------------+-------------------------+--------------+-------------+----------------------+-----------+
    only showing top 10 rows
    



```python

```
