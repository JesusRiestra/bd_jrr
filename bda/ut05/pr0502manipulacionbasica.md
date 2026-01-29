```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, lower, like, isnotnull
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
    26/01/29 09:24:38 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


    SparkSession iniciada correctamente!



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
# 1
df_sel = df_cultivos.select("Crop", "Region", "Temperature_C", "Rainfall_mm", "Irrigation", "Yield_ton_per_ha")
```


```python
# 2
df_renamed = df_sel \
    .withColumnRenamed("Temperature_C", "Temperatura") \
    .withColumnRenamed("Rainfall_mm", "Lluvia") \
    .withColumnRenamed("Yield_ton_per_ha", "Rendimiento")
```


```python
# 3
df_renamed = df_renamed.where(
    (col("Crop") == "Maize") & (col("Temperatura") > 25))
df_renamed.show(10)
```

    26/01/29 09:24:50 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors
                                                                                    

    +-----+--------+-----------+------+----------+-----------+
    | Crop|  Region|Temperatura|Lluvia|Irrigation|Rendimiento|
    +-----+--------+-----------+------+----------+-----------+
    |Maize|Region_D|       26.4|1054.3|      Drip|     169.06|
    |Maize|Region_C|       32.4| 846.1|      None|      162.2|
    |Maize|Region_A|       26.6| 362.5| Sprinkler|      95.23|
    |Maize|Region_C|       33.7|1193.3|      None|     110.57|
    |Maize|Region_C|       27.8| 695.2|     Flood|     143.84|
    |Maize|Region_D|       30.2|1001.4|     Flood|     138.61|
    |Maize|Region_A|       27.7| 747.7| Sprinkler|     114.58|
    |Maize|Region_B|       28.9|1392.9|      Drip|     169.23|
    |Maize|Region_B|       34.7| 694.4|      Drip|      96.08|
    |Maize|Region_D|       29.5| 848.8|     Flood|      93.45|
    +-----+--------+-----------+------+----------+-----------+
    only showing top 10 rows
    



```python
# 4
df = (df_cultivos \
        .select("Crop", "Region", "Temperature_C", "Rainfall_mm", "Irrigation", "Yield_ton_per_ha") \
        .withColumnRenamed("Temperature_C", "Temperatura") \
        .withColumnRenamed("Rainfall_mm", "Lluvia") \
        .withColumnRenamed("Yield_ton_per_ha", "Rendimiento") \
        .where(
            (col("Crop") == "Maize") & (col("Temperatura") > 25))
     )
df.show(10)
```

    +-----+--------+-----------+------+----------+-----------+
    | Crop|  Region|Temperatura|Lluvia|Irrigation|Rendimiento|
    +-----+--------+-----------+------+----------+-----------+
    |Maize|Region_D|       26.4|1054.3|      Drip|     169.06|
    |Maize|Region_C|       32.4| 846.1|      None|      162.2|
    |Maize|Region_A|       26.6| 362.5| Sprinkler|      95.23|
    |Maize|Region_C|       33.7|1193.3|      None|     110.57|
    |Maize|Region_C|       27.8| 695.2|     Flood|     143.84|
    |Maize|Region_D|       30.2|1001.4|     Flood|     138.61|
    |Maize|Region_A|       27.7| 747.7| Sprinkler|     114.58|
    |Maize|Region_B|       28.9|1392.9|      Drip|     169.23|
    |Maize|Region_B|       34.7| 694.4|      Drip|      96.08|
    |Maize|Region_D|       29.5| 848.8|     Flood|      93.45|
    +-----+--------+-----------+------+----------+-----------+
    only showing top 10 rows
    



```python
# - - - DATASET 2 - - -
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
df_base = df_lugares.select("Place_Name", "Country", "UNESCO_World_Heritage", "Entry_Fee_USD", "Annual_Visitors_Millions")
```


```python
# 2
df_es = df_base \
    .withColumnRenamed("Place_Name", "Lugar") \
    .withColumnRenamed("UNESCO_World_Heritage", "Es_UNESCO") \
    .withColumnRenamed("Entry_Fee_USD", "Precio_Entrada") \
    .withColumnRenamed("Annual_Visitors_Millions", "Visitantes_Millones")
```


```python
# 3
df_es = df_es.where(
    (col("Es_UNESCO") == "Yes") & (col("Precio_Entrada") <= 20)
)
df_es.show(10)
```

    +--------------------+--------------+---------+--------------+-------------------+
    |               Lugar|       Country|Es_UNESCO|Precio_Entrada|Visitantes_Millones|
    +--------------------+--------------+---------+--------------+-------------------+
    | Great Wall of China|         China|      Yes|          10.0|               10.0|
    |           Taj Mahal|         India|      Yes|          15.0|                7.5|
    |           Colosseum|         Italy|      Yes|          18.0|               7.65|
    |      Forbidden City|         China|      Yes|           8.0|                9.0|
    |Notre-Dame Cathedral|        France|      Yes|           0.0|               13.0|
    |Great Pyramid of ...|         Egypt|      Yes|          20.0|                2.8|
    |Leaning Tower of ...|         Italy|      Yes|          20.0|                5.0|
    |           Acropolis|        Greece|      Yes|          13.0|                4.0|
    |             Big Ben|United Kingdom|      Yes|           0.0|                5.5|
    +--------------------+--------------+---------+--------------+-------------------+
    



```python
# - - - DATASET 3 - - -
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
                    .option("delimiter", ";")
                    .load("./registro-de-turismo-de-castilla-y-leon.csv")
                )
```


```python
# 1
df_contactos = df_turismo.select("nombre", "tipo", "provincia", "web", "email")
```


```python
df_contactos.show(20)
```

    +--------------------+--------------------+---------+--------------------+--------------------+
    |              nombre|                tipo|provincia|                 web|               email|
    +--------------------+--------------------+---------+--------------------+--------------------+
    |BERNARDO MORO MEN...|Profesional de Tu...| Asturias|                NULL|bernardomoro@hotm...|
    |        LA SASTRERÍA|Casa Rural de Alq...|    Ávila|www.lasastreriade...|                NULL|
    |         LAS HAZANAS|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    | LA CASITA DEL PAJAR|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    |            MARACANA|                 Bar|    Ávila|                NULL|emo123anatoliev@g...|
    |               PLAZA|                 Bar|    Ávila|                NULL|                NULL|
    |          LA OFICINA|                 Bar|    Ávila|                NULL|                NULL|
    |              JARDIN|                 Bar|    Ávila|                NULL|                NULL|
    |               CESAR|           Cafetería|    Ávila|                NULL|                NULL|
    |             ADANERO|           Cafetería|    Ávila|                NULL|                NULL|
    |                  MA|   Restaurante / Bar|    Ávila|                NULL|info@restaurantem...|
    |chalet encantador...|               Chalé|    Ávila|                NULL|correcaminos_ssp@...|
    |  FINCA AGUATACHALES|    Inmueble análogo|    Ávila|                NULL|fausto.saez.illob...|
    |JARDIN BOTÁNICO V...|p - Actividades d...|    Ávila|WWW.JARDINTIETAR.COM|AXEL.MAHLAU@GMAIL...|
    |P.INFORMACIÓN T. ...|n - Oficinas y pu...|    Ávila|                NULL|oficinaturismolaa...|
    |          LA ESPUELA|  Albergue turístico|    Ávila|www.turismorurall...|info@turismorural...|
    |           CONCEJO I|Casa Rural de Alq...|    Ávila|                NULL|                NULL|
    |       EL TIO MORENO|          Casa Rural|    Ávila|                NULL|   jgabari@gmail.com|
    |          CONCEJO II|Casa Rural de Alq...|    Ávila|                NULL|                NULL|
    |         LOS ABUELOS|Casa Rural de Alq...|    Ávila|                NULL|bonyros@telefonic...|
    +--------------------+--------------------+---------+--------------------+--------------------+
    only showing top 20 rows
    



```python
# 2
df_limpio = df_contactos \
    .withColumnRenamed("nombre", "nombre_establecimiento") \
    .withColumnRenamed("tipo", "categoria_actividad") \
    .withColumnRenamed("web", "sitio_web") \
    .withColumnRenamed("email", "correo_electronico")
```


```python
df_limpio.show(20)
```

    +----------------------+--------------------+---------+--------------------+--------------------+
    |nombre_establecimiento| categoria_actividad|provincia|           sitio_web|  correo_electronico|
    +----------------------+--------------------+---------+--------------------+--------------------+
    |  BERNARDO MORO MEN...|Profesional de Tu...| Asturias|                NULL|bernardomoro@hotm...|
    |          LA SASTRERÍA|Casa Rural de Alq...|    Ávila|www.lasastreriade...|                NULL|
    |           LAS HAZANAS|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    |   LA CASITA DEL PAJAR|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    |              MARACANA|                 Bar|    Ávila|                NULL|emo123anatoliev@g...|
    |                 PLAZA|                 Bar|    Ávila|                NULL|                NULL|
    |            LA OFICINA|                 Bar|    Ávila|                NULL|                NULL|
    |                JARDIN|                 Bar|    Ávila|                NULL|                NULL|
    |                 CESAR|           Cafetería|    Ávila|                NULL|                NULL|
    |               ADANERO|           Cafetería|    Ávila|                NULL|                NULL|
    |                    MA|   Restaurante / Bar|    Ávila|                NULL|info@restaurantem...|
    |  chalet encantador...|               Chalé|    Ávila|                NULL|correcaminos_ssp@...|
    |    FINCA AGUATACHALES|    Inmueble análogo|    Ávila|                NULL|fausto.saez.illob...|
    |  JARDIN BOTÁNICO V...|p - Actividades d...|    Ávila|WWW.JARDINTIETAR.COM|AXEL.MAHLAU@GMAIL...|
    |  P.INFORMACIÓN T. ...|n - Oficinas y pu...|    Ávila|                NULL|oficinaturismolaa...|
    |            LA ESPUELA|  Albergue turístico|    Ávila|www.turismorurall...|info@turismorural...|
    |             CONCEJO I|Casa Rural de Alq...|    Ávila|                NULL|                NULL|
    |         EL TIO MORENO|          Casa Rural|    Ávila|                NULL|   jgabari@gmail.com|
    |            CONCEJO II|Casa Rural de Alq...|    Ávila|                NULL|                NULL|
    |           LOS ABUELOS|Casa Rural de Alq...|    Ávila|                NULL|bonyros@telefonic...|
    +----------------------+--------------------+---------+--------------------+--------------------+
    only showing top 20 rows
    



```python
# 3
df_final = df_limpio \
    .where(
        (col("provincia") == "Burgos") & (col("categoria_actividad").like("%Bodegas%") & (col("sitio_web").isNotNull()))
    )
```


```python
df_final.show(20)
```

    +----------------------+--------------------+---------+--------------------+--------------------+
    |nombre_establecimiento| categoria_actividad|provincia|           sitio_web|  correo_electronico|
    +----------------------+--------------------+---------+--------------------+--------------------+
    |        BODEGAS TARSUS|g - Bodegas y los...|   Burgos|  www.tarsusvino.com|                NULL|
    |  BODEGAS DOMINIO D...|g - Bodegas y los...|   Burgos|www.dominiodecair...|bodegas@dominiode...|
    |    TERRITORIO LUTHIER|g - Bodegas y los...|   Burgos|territorioluthier...|luthier@territori...|
    |    BODEGA COVARRUBIAS|g - Bodegas y los...|   Burgos| http://valdable.com|   info@valdable.com|
    |  BODEGAS PASCUAL, ...|g - Bodegas y los...|   Burgos|222.bodegaspascua...|export@bodegaspas...|
    |   BODEGAS VINUM VITAE|g - Bodegas y los...|   Burgos|      www.avañate.es|vinum.vitae.bodeg...|
    |  VIÑEDOS Y BODEGAS...|g - Bodegas y los...|   Burgos|     www.ferratus.es|administracion@fe...|
    |  BODEGAS Y VIÑEDOS...|g - Bodegas y los...|   Burgos|     www.pradorey.es|   info@pradorey.com|
    |       BODEGAS ARROCAL|g - Bodegas y los...|   Burgos|     www.arrocal.com|  blanca@arrocal.com|
    |           VIÑA ARNÁIZ|g - Bodegas y los...|   Burgos|  www.vinaarnaiz.com|   enoturismo@jgc.es|
    |  BODEGAS PALACIO D...|g - Bodegas y los...|   Burgos|www.palaciodelerm...|info@palaciodeler...|
    |    BODEGAS MONTE AMÁN|g - Bodegas y los...|   Burgos|   www.monteaman.com|bodegas@monteaman...|
    |         ALONSO ANGULO|g - Bodegas y los...|   Burgos|www.alonsoangulo.com|info@alonsoangulo...|
    |  VIÑA MAMBRILLA, S.L.|g - Bodegas y los...|   Burgos|   www.mambrilla.com| bodegamambrilla.com|
    |  BODEGAS TRASLASCU...|g - Bodegas y los...|   Burgos|www.bodegastrasla...|administracion@bo...|
    |  BODEGAS RODERO, S.L.|g - Bodegas y los...|   Burgos|www.bodegasrodero...|rodero@bodegasrod...|
    |  BODEGAS HERMANOS ...|g - Bodegas y los...|   Burgos|www.perezpascuas.com|viñapedrosa@perez...|
    |    BOSQUE DE MATASNOS|g - Bodegas y los...|   Burgos|https://bosquedem...|administracion@bo...|
    |  BODEGAS PRADO DE ...|g - Bodegas y los...|   Burgos|www.pradodeolmedo...|pradodeolmedo@pra...|
    |  VISITAS ENOTURÍST...|g - Bodegas y los...|   Burgos|www.lopezcristoba...|bodega@lopezcrist...|
    +----------------------+--------------------+---------+--------------------+--------------------+
    only showing top 20 rows
    



```python

```
