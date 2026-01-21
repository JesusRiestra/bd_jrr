```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from pyspark.sql import SparkSession
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
# - - - CULTIVOS - - -
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
df_cultivos.printSchema()
df_cultivos.show(5)
```

    root
     |-- Crop: string (nullable = true)
     |-- Region: string (nullable = true)
     |-- Soil_Type: string (nullable = true)
     |-- Soil_pH: double (nullable = true)
     |-- Rainfall_mm: double (nullable = true)
     |-- Temperature_C: double (nullable = true)
     |-- Humidity_pct: double (nullable = true)
     |-- Fertilizer_Used_kg: double (nullable = true)
     |-- Irrigation: string (nullable = true)
     |-- Pesticides_Used_kg: double (nullable = true)
     |-- Planting_Density: double (nullable = true)
     |-- Previous_Crop: string (nullable = true)
     |-- Yield_ton_per_ha: double (nullable = true)
    
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+
    |  Crop|  Region|Soil_Type|Soil_pH|Rainfall_mm|Temperature_C|Humidity_pct|Fertilizer_Used_kg|Irrigation|Pesticides_Used_kg|Planting_Density|Previous_Crop|Yield_ton_per_ha|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+
    | Maize|Region_C|    Sandy|   7.01|     1485.4|         19.7|        40.3|             105.1|      Drip|              10.2|            23.2|         Rice|          101.48|
    |Barley|Region_D|     Loam|   5.79|      399.4|         29.1|        55.4|             221.8| Sprinkler|              35.5|             7.4|       Barley|          127.39|
    |  Rice|Region_C|     Clay|   7.24|      980.9|         30.5|        74.4|              61.2| Sprinkler|              40.0|             5.1|        Wheat|           68.99|
    | Maize|Region_D|     Loam|   6.79|     1054.3|         26.4|        62.0|             257.8|      Drip|              42.7|            23.7|         None|          169.06|
    | Maize|Region_D|    Sandy|   5.96|      744.6|         20.4|        70.9|             195.8|      Drip|              25.5|            15.6|        Maize|          118.71|
    +------+--------+---------+-------+-----------+-------------+------------+------------------+----------+------------------+----------------+-------------+----------------+
    only showing top 5 rows
    



```python
# - - - LUGARES FAMOSOS - - -
esquema_lugares = StructType([
    StructField("Place_Name", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Annual_Visitors_Millions", DoubleType(), True),
    StructField("Type", StringType(), True),
    StructField("UNESCO_World_Heritage", BooleanType(), True),
    StructField("Year_Built", StringType(), True),
    StructField("Entry_Fee_USD", DoubleType(), True),
    StructField("Best_Visit_Month", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Tourism_Revenue_Million_USD", DoubleType(), True),
    StructField("Average_Visit_Duration_Hours", DoubleType(), True),
    StructField("Famous_For", StringType(), True)
])
```


```python
df_lugares = ( spark.read
                    .format("csv")
                    .schema(esquema_lugares)
                    .option("header", "true")
                    .option("quote", "\"")
                    .load("./world_famous_places_2024.csv")
                )
```


```python
df_lugares.printSchema()
df_lugares.show(5)
```

    root
     |-- Place_Name: string (nullable = true)
     |-- Country: string (nullable = true)
     |-- City: string (nullable = true)
     |-- Annual_Visitors_Millions: double (nullable = true)
     |-- Type: string (nullable = true)
     |-- UNESCO_World_Heritage: boolean (nullable = true)
     |-- Year_Built: string (nullable = true)
     |-- Entry_Fee_USD: double (nullable = true)
     |-- Best_Visit_Month: string (nullable = true)
     |-- Region: string (nullable = true)
     |-- Tourism_Revenue_Million_USD: double (nullable = true)
     |-- Average_Visit_Duration_Hours: double (nullable = true)
     |-- Famous_For: string (nullable = true)
    
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+
    |         Place_Name|      Country|            City|Annual_Visitors_Millions|              Type|UNESCO_World_Heritage|      Year_Built|Entry_Fee_USD| Best_Visit_Month|        Region|Tourism_Revenue_Million_USD|Average_Visit_Duration_Hours|          Famous_For|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+
    |       Eiffel Tower|       France|           Paris|                     7.0|    Monument/Tower|                 NULL|            1889|         35.0|May-June/Sept-Oct|Western Europe|                       95.0|                         2.5|Iconic iron latti...|
    |       Times Square|United States|   New York City|                    50.0|    Urban Landmark|                 NULL|            1904|          0.0|Apr-June/Sept-Nov| North America|                       70.0|                         1.5|Bright lights, Br...|
    |      Louvre Museum|       France|           Paris|                     8.7|            Museum|                 NULL|            1793|         22.0|        Oct-March|Western Europe|                      120.0|                         4.0|World's most visi...|
    |Great Wall of China|        China|Beijing/Multiple|                    10.0| Historic Monument|                 NULL|220 BC - 1644 AD|         10.0| Apr-May/Sept-Oct|     East Asia|                      180.0|                         4.0|Ancient defensive...|
    |          Taj Mahal|        India|            Agra|                     7.5|Monument/Mausoleum|                 NULL|            1653|         15.0|        Oct-March|    South Asia|                       65.0|                         2.0|White marble maus...|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+
    only showing top 5 rows
    



```python
# - - - Turismo - - -
esquema_turismo = StructType([
    StructField("establecimiento", StringType(), True),
    StructField("n_registro", StringType(), True),
    StructField("codigo", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("especialidades", StringType(), True),
    StructField("clase", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("c_postal", IntegerType(), True),
    StructField("provincia", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("localidad", StringType(), True),
    StructField("nucleo", StringType(), True),
    StructField("telefono_1", StringType(), True),
    StructField("telefono_2", StringType(), True),
    StructField("telefono_3", StringType(), True),
    StructField("email", StringType(), True),
    StructField("web", StringType(), True),
    StructField("q_calidad", StringType(), True),
    StructField("posada_real", StringType(), True),
    StructField("plazas", IntegerType(), True),
    StructField("gps_longitud", DoubleType(), True),
    StructField("gps_latitud", DoubleType(), True),
    StructField("personas_con_discapacidad", StringType(), True),
    StructField("column_27", StringType(), True),
    StructField("posicion", StringType(), True)
])
```


```python
df_turismo = ( spark.read
                    .format("csv")
                    .schema(esquema_turismo)
                    .option("header", "true")
                    .option("quote", "\"")
                    .load("./registro-de-turismo-de-castilla-y-leon.csv")
                )
```


```python
df_turismo.printSchema()
df_turismo.show(5)
```

    root
     |-- establecimiento: string (nullable = true)
     |-- n_registro: string (nullable = true)
     |-- codigo: string (nullable = true)
     |-- tipo: string (nullable = true)
     |-- categoria: string (nullable = true)
     |-- especialidades: string (nullable = true)
     |-- clase: string (nullable = true)
     |-- nombre: string (nullable = true)
     |-- direccion: string (nullable = true)
     |-- c_postal: integer (nullable = true)
     |-- provincia: string (nullable = true)
     |-- municipio: string (nullable = true)
     |-- localidad: string (nullable = true)
     |-- nucleo: string (nullable = true)
     |-- telefono_1: string (nullable = true)
     |-- telefono_2: string (nullable = true)
     |-- telefono_3: string (nullable = true)
     |-- email: string (nullable = true)
     |-- web: string (nullable = true)
     |-- q_calidad: string (nullable = true)
     |-- posada_real: string (nullable = true)
     |-- plazas: integer (nullable = true)
     |-- gps_longitud: double (nullable = true)
     |-- gps_latitud: double (nullable = true)
     |-- personas_con_discapacidad: string (nullable = true)
     |-- column_27: string (nullable = true)
     |-- posicion: string (nullable = true)
    
    +--------------------+-----------+------+----+---------+--------------+-----+------+---------+--------+---------+---------+---------+------+----------+----------+----------+-----+----+---------+-----------+------+------------+-----------+-------------------------+---------+--------+
    |     establecimiento| n_registro|codigo|tipo|categoria|especialidades|clase|nombre|direccion|c_postal|provincia|municipio|localidad|nucleo|telefono_1|telefono_2|telefono_3|email| web|q_calidad|posada_real|plazas|gps_longitud|gps_latitud|personas_con_discapacidad|column_27|posicion|
    +--------------------+-----------+------+----+---------+--------------+-----+------+---------+--------+---------+---------+---------+------+----------+----------+----------+-----+----+---------+-----------+------+------------+-----------+-------------------------+---------+--------+
    |Turismo Activo;47...|       NULL|  NULL|NULL|     NULL|          NULL| NULL|  NULL|     NULL|    NULL|     NULL|     NULL|     NULL|  NULL|      NULL|      NULL|      NULL| NULL|NULL|     NULL|       NULL|  NULL|        NULL|       NULL|                     NULL|     NULL|    NULL|
    |Alojam. Turismo R...|       NULL|  NULL|NULL|     NULL|          NULL| NULL|  NULL|     NULL|    NULL|     NULL|     NULL|     NULL|  NULL|      NULL|      NULL|      NULL| NULL|NULL|     NULL|       NULL|  NULL|        NULL|       NULL|                     NULL|     NULL|    NULL|
    |Alojam. Turismo R...| -4.6033331|  NULL|NULL|     NULL|          NULL| NULL|  NULL|     NULL|    NULL|     NULL|     NULL|     NULL|  NULL|      NULL|      NULL|      NULL| NULL|NULL|     NULL|       NULL|  NULL|        NULL|       NULL|                     NULL|     NULL|    NULL|
    |Alojam. Turismo R...| -4.6033333|  NULL|NULL|     NULL|          NULL| NULL|  NULL|     NULL|    NULL|     NULL|     NULL|     NULL|  NULL|      NULL|      NULL|      NULL| NULL|NULL|     NULL|       NULL|  NULL|        NULL|       NULL|                     NULL|     NULL|    NULL|
    |Bares;05/002525;;...|       NULL|  NULL|NULL|     NULL|          NULL| NULL|  NULL|     NULL|    NULL|     NULL|     NULL|     NULL|  NULL|      NULL|      NULL|      NULL| NULL|NULL|     NULL|       NULL|  NULL|        NULL|       NULL|                     NULL|     NULL|    NULL|
    +--------------------+-----------+------+----+---------+--------------+-----+------+---------+--------+---------+---------+---------+------+----------+----------+----------+-----+----+---------+-----------+------+------------+-----------+-------------------------+---------+--------+
    only showing top 5 rows
    

