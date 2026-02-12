```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, concat_ws,lit, lpad, rpad, split, when, lower, upper, log, round, bround, greatest, to_date, date_add, month, substring, ceil, log10, least, regexp_replace, to_date, date_add, date_diff
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
    26/02/12 08:41:55 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


    SparkSession iniciada correctamente!



```python
# - - - ESQUEMA LUGARES FAMOSOS - - -
esquema_lugares = StructType([
    StructField("Place_Name", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Annual_Visitors_Millions", DoubleType(), True),
    StructField("Type", StringType(), True),
    StructField("UNESCO_World_Heritage", StringType(), True),
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
# 1
df_feat = df_lugares.withColumn(
            "SKU_Lugar",
            concat_ws(
                "_",
                upper(substring(col("Country"), 1, 3)),
                rpad(substring(col("City"), 1, 3), 3, "X"),
                split(col("Type"), "/")[0]
            )
)

df_feat.show(5)
```

    26/02/12 08:42:09 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors
                                                                                    

    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+
    |         Place_Name|      Country|            City|Annual_Visitors_Millions|              Type|UNESCO_World_Heritage|      Year_Built|Entry_Fee_USD| Best_Visit_Month|        Region|Tourism_Revenue_Million_USD|Average_Visit_Duration_Hours|          Famous_For|           SKU_Lugar|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+
    |       Eiffel Tower|       France|           Paris|                     7.0|    Monument/Tower|                   No|            1889|         35.0|May-June/Sept-Oct|Western Europe|                       95.0|                         2.5|Iconic iron latti...|    FRA_Par_Monument|
    |       Times Square|United States|   New York City|                    50.0|    Urban Landmark|                   No|            1904|          0.0|Apr-June/Sept-Nov| North America|                       70.0|                         1.5|Bright lights, Br...|UNI_New_Urban Lan...|
    |      Louvre Museum|       France|           Paris|                     8.7|            Museum|                  Yes|            1793|         22.0|        Oct-March|Western Europe|                      120.0|                         4.0|World's most visi...|      FRA_Par_Museum|
    |Great Wall of China|        China|Beijing/Multiple|                    10.0| Historic Monument|                  Yes|220 BC - 1644 AD|         10.0| Apr-May/Sept-Oct|     East Asia|                      180.0|                         4.0|Ancient defensive...|CHI_Bei_Historic ...|
    |          Taj Mahal|        India|            Agra|                     7.5|Monument/Mausoleum|                  Yes|            1653|         15.0|        Oct-March|    South Asia|                       65.0|                         2.0|White marble maus...|    IND_Agr_Monument|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+
    only showing top 5 rows
    



```python
# 2
df_feat = df_feat \
        .withColumn(
            "Duracion_Techo",
            ceil(col("Average_Visit_Duration_Hours"))
        ).withColumn(
            "Log_Ingresos",
            log10(col("Tourism_Revenue_Million_USD"))
        ).withColumn(
            "Mejor_Oferta",
            least(col("Entry_Fee_USD"), lit(20))
        )

df_feat.show(5)
```

    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+
    |         Place_Name|      Country|            City|Annual_Visitors_Millions|              Type|UNESCO_World_Heritage|      Year_Built|Entry_Fee_USD| Best_Visit_Month|        Region|Tourism_Revenue_Million_USD|Average_Visit_Duration_Hours|          Famous_For|           SKU_Lugar|Duracion_Techo|      Log_Ingresos|Mejor_Oferta|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+
    |       Eiffel Tower|       France|           Paris|                     7.0|    Monument/Tower|                   No|            1889|         35.0|May-June/Sept-Oct|Western Europe|                       95.0|                         2.5|Iconic iron latti...|    FRA_Par_Monument|             3|1.9777236052888478|        20.0|
    |       Times Square|United States|   New York City|                    50.0|    Urban Landmark|                   No|            1904|          0.0|Apr-June/Sept-Nov| North America|                       70.0|                         1.5|Bright lights, Br...|UNI_New_Urban Lan...|             2| 1.845098040014257|         0.0|
    |      Louvre Museum|       France|           Paris|                     8.7|            Museum|                  Yes|            1793|         22.0|        Oct-March|Western Europe|                      120.0|                         4.0|World's most visi...|      FRA_Par_Museum|             4|2.0791812460476247|        20.0|
    |Great Wall of China|        China|Beijing/Multiple|                    10.0| Historic Monument|                  Yes|220 BC - 1644 AD|         10.0| Apr-May/Sept-Oct|     East Asia|                      180.0|                         4.0|Ancient defensive...|CHI_Bei_Historic ...|             4| 2.255272505103306|        10.0|
    |          Taj Mahal|        India|            Agra|                     7.5|Monument/Mausoleum|                  Yes|            1653|         15.0|        Oct-March|    South Asia|                       65.0|                         2.0|White marble maus...|    IND_Agr_Monument|             2|1.8129133566428555|        15.0|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+
    only showing top 5 rows
    



```python
# 3
df_feat = df_feat \
    .withColumn(
        "Desc_Corta",
        substring(col("Famous_For"), 1, 15)
    ).withColumn(
        "Ciudad_Limpia",
        regexp_replace(col("City"), "New York City", "NYC")
    )

df_feat.show(5)
```

    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+---------------+----------------+
    |         Place_Name|      Country|            City|Annual_Visitors_Millions|              Type|UNESCO_World_Heritage|      Year_Built|Entry_Fee_USD| Best_Visit_Month|        Region|Tourism_Revenue_Million_USD|Average_Visit_Duration_Hours|          Famous_For|           SKU_Lugar|Duracion_Techo|      Log_Ingresos|Mejor_Oferta|     Desc_Corta|   Ciudad_Limpia|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+---------------+----------------+
    |       Eiffel Tower|       France|           Paris|                     7.0|    Monument/Tower|                   No|            1889|         35.0|May-June/Sept-Oct|Western Europe|                       95.0|                         2.5|Iconic iron latti...|    FRA_Par_Monument|             3|1.9777236052888478|        20.0|Iconic iron lat|           Paris|
    |       Times Square|United States|   New York City|                    50.0|    Urban Landmark|                   No|            1904|          0.0|Apr-June/Sept-Nov| North America|                       70.0|                         1.5|Bright lights, Br...|UNI_New_Urban Lan...|             2| 1.845098040014257|         0.0|Bright lights, |             NYC|
    |      Louvre Museum|       France|           Paris|                     8.7|            Museum|                  Yes|            1793|         22.0|        Oct-March|Western Europe|                      120.0|                         4.0|World's most visi...|      FRA_Par_Museum|             4|2.0791812460476247|        20.0|World's most vi|           Paris|
    |Great Wall of China|        China|Beijing/Multiple|                    10.0| Historic Monument|                  Yes|220 BC - 1644 AD|         10.0| Apr-May/Sept-Oct|     East Asia|                      180.0|                         4.0|Ancient defensive...|CHI_Bei_Historic ...|             4| 2.255272505103306|        10.0|Ancient defensi|Beijing/Multiple|
    |          Taj Mahal|        India|            Agra|                     7.5|Monument/Mausoleum|                  Yes|            1653|         15.0|        Oct-March|    South Asia|                       65.0|                         2.0|White marble maus...|    IND_Agr_Monument|             2|1.8129133566428555|        15.0|White marble ma|            Agra|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+---------------+----------------+
    only showing top 5 rows
    



```python
# 4
df_feat = df_feat \
    .withColumn(
        "Inicio_Campana",
        to_date(lit("2024-06-01"))
    ).withColumn(
        "Fin_Campana",
        date_add(col("Inicio_Campana"), 90)
    ).withColumn(
        "Fecha_Construccion",
        to_date(concat(col("Year_Built"), lit("-01-01")))
    ).withColumn(
        "Dias_Hasta_Fin",
        date_diff(col("Fin_Campana"), col("Fecha_Construccion"))
    )

df_feat.show(5)
```

    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+---------------+----------------+--------------+-----------+--------------+------------------+
    |         Place_Name|      Country|            City|Annual_Visitors_Millions|              Type|UNESCO_World_Heritage|      Year_Built|Entry_Fee_USD| Best_Visit_Month|        Region|Tourism_Revenue_Million_USD|Average_Visit_Duration_Hours|          Famous_For|           SKU_Lugar|Duracion_Techo|      Log_Ingresos|Mejor_Oferta|     Desc_Corta|   Ciudad_Limpia|Inicio_Campana|Fin_Campana|Dias_Hasta_Fin|Fecha_Construccion|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+---------------+----------------+--------------+-----------+--------------+------------------+
    |       Eiffel Tower|       France|           Paris|                     7.0|    Monument/Tower|                   No|            1889|         35.0|May-June/Sept-Oct|Western Europe|                       95.0|                         2.5|Iconic iron latti...|    FRA_Par_Monument|             3|1.9777236052888478|        20.0|Iconic iron lat|           Paris|    2024-06-01| 2024-08-30|         49549|        1889-01-01|
    |       Times Square|United States|   New York City|                    50.0|    Urban Landmark|                   No|            1904|          0.0|Apr-June/Sept-Nov| North America|                       70.0|                         1.5|Bright lights, Br...|UNI_New_Urban Lan...|             2| 1.845098040014257|         0.0|Bright lights, |             NYC|    2024-06-01| 2024-08-30|         44072|        1904-01-01|
    |      Louvre Museum|       France|           Paris|                     8.7|            Museum|                  Yes|            1793|         22.0|        Oct-March|Western Europe|                      120.0|                         4.0|World's most visi...|      FRA_Par_Museum|             4|2.0791812460476247|        20.0|World's most vi|           Paris|    2024-06-01| 2024-08-30|         84612|        1793-01-01|
    |Great Wall of China|        China|Beijing/Multiple|                    10.0| Historic Monument|                  Yes|220 BC - 1644 AD|         10.0| Apr-May/Sept-Oct|     East Asia|                      180.0|                         4.0|Ancient defensive...|CHI_Bei_Historic ...|             4| 2.255272505103306|        10.0|Ancient defensi|Beijing/Multiple|    2024-06-01| 2024-08-30|          NULL|              NULL|
    |          Taj Mahal|        India|            Agra|                     7.5|Monument/Mausoleum|                  Yes|            1653|         15.0|        Oct-March|    South Asia|                       65.0|                         2.0|White marble maus...|    IND_Agr_Monument|             2|1.8129133566428555|        15.0|White marble ma|            Agra|    2024-06-01| 2024-08-30|        135746|        1653-01-01|
    +-------------------+-------------+----------------+------------------------+------------------+---------------------+----------------+-------------+-----------------+--------------+---------------------------+----------------------------+--------------------+--------------------+--------------+------------------+------------+---------------+----------------+--------------+-----------+--------------+------------------+
    only showing top 5 rows
    

