```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, DateType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
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

    SparkSession iniciada correctamente!



```python
# - - - ESQUEMA COMPORTAMIENTO USUARIOS DE NETFLIX - - -
esquema_peliculas = StructType([
    StructField("row_id", IntegerType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("duration", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("release_date", DateType(), True),
    StructField("movie_id", StringType(), True),
    StructField("user_id", StringType(), True),
])
```


```python
df_peliculas = ( spark.read
             .format("csv")
             .schema(esquema_peliculas)
             .option("header", "true")
             .option("quote", "\"")
             .load("vodclickstream_uk_movies_03.csv")
)
```


```python
df_peliculas.show()
```

    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+
    |row_id|           datetime|duration|               title|              genres|release_date|  movie_id|   user_id|
    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+
    | 58773|2017-01-01 01:15:09|     0.0|Angus, Thongs and...|Comedy, Drama, Ro...|  2008-07-25|26bd5987e8|1dea19f6fe|
    | 58774|2017-01-01 13:56:02|     0.0|The Curse of Slee...|Fantasy, Horror, ...|  2016-06-02|f26ed2675e|544dcbc510|
    | 58775|2017-01-01 15:17:47| 10530.0|   London Has Fallen|    Action, Thriller|  2016-03-04|f77e500e7a|7cbcc791bf|
    | 58776|2017-01-01 16:04:13|    49.0|            Vendetta|       Action, Drama|  2015-06-12|c74aec7673|ebf43c36b6|
    | 58777|2017-01-01 19:16:37|     0.0|The SpongeBob Squ...|Animation, Action...|  2004-11-19|a80d6fc2aa|a57c992287|
    | 58778|2017-01-01 19:21:37|     0.0|   London Has Fallen|    Action, Thriller|  2016-03-04|f77e500e7a|c5bf4f3f57|
    | 58779|2017-01-01 19:43:06|  4903.0|   The Water Diviner| Drama, History, War|  2014-12-26|7165c2fc94|8e1be40e32|
    | 58780|2017-01-01 19:44:38|     0.0|  Angel of Christmas|     Comedy, Romance|  2015-11-29|b2f02f2689|892a51dee1|
    | 58781|2017-01-01 19:46:24|  3845.0|              Ratter|Drama, Horror, Th...|  2016-02-12|c39aae36c3|cff8ea652a|
    | 58782|2017-01-01 20:27:04|     0.0|    The Book of Life|Animation, Advent...|  2014-10-17|97183b9136|bf53608c70|
    | 58783|2017-01-01 20:54:56|     0.0|       November Rule|              Comedy|  2015-02-14|336689ad43|059f371467|
    | 58784|2017-01-01 20:55:46|  6175.0|             28 Days|       Comedy, Drama|  2000-04-14|584bffaf5f|759ae2eac9|
    | 58785|2017-01-01 21:25:03|     0.0|     Eddie the Eagle|Biography, Comedy...|  2016-02-26|339460099f|37b4e3f352|
    | 58786|2017-01-01 21:33:26| 38120.0|The SpongeBob Squ...|Animation, Action...|  2004-11-19|a80d6fc2aa|5b1727dc12|
    | 58787|2017-01-01 21:37:41|  7799.0| Beasts of No Nation|          Drama, War|  2015-10-16|c57e11da52|3142b4c730|
    | 58788|2017-01-01 21:59:56|     0.0|        At All Costs|  Documentary, Sport|  2016-09-30|b4c76a86f3|b25918e854|
    | 58789|2017-01-01 22:51:50|     0.0|      The Great Raid|  Action, Drama, War|  2005-08-12|039b55dbba|2c66cc607c|
    | 58790|2017-01-01 22:52:08|   181.0|           Maravilla|Documentary, Biog...|  2014-05-29|19c537b01e|93e9369e81|
    | 58791|2017-01-01 22:54:56|     0.0|               Creed|        Drama, Sport|  2015-11-25|d2b995fcb7|5e05d4a6ba|
    | 58792|2017-01-01 00:19:40| 54195.0|    About Last Night|     Comedy, Romance|  2014-02-14|f7d088d208|78cdb81c4f|
    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+
    only showing top 20 rows
    



```python
# 1
ventana_tiempo = Window.partitionBy("user_id").orderBy("datetime")

df_peliculas = df_peliculas.withColumn(
    "next_datetime", F.lead("datetime").over(ventana_tiempo)
).withColumn(
        "calculated_time_to_next", F.col("next_datetime").cast("long") - F.col("datetime").cast("long")
)

df_peliculas.show()
```

    [Stage 112:=================================================>       (7 + 1) / 8]

    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+-------------------+-----------------------+
    |row_id|           datetime|duration|               title|              genres|release_date|  movie_id|   user_id|      next_datetime|calculated_time_to_next|
    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+-------------------+-----------------------+
    |139643|2017-05-19 20:21:43|     0.0|                XOXO|        Drama, Music|  2016-08-26|7369676dec|0006ea6b5c|2017-05-20 21:54:34|                  91971|
    |140442|2017-05-20 21:54:34|     0.0|            Hot Fuzz|Action, Comedy, M...|  2007-04-20|6467fee6b6|0006ea6b5c|2017-05-26 18:38:01|                 506607|
    |144717|2017-05-26 18:38:01|     0.0|         War Machine|  Comedy, Drama, War|  2017-05-26|0f3b137f4e|0006ea6b5c|2017-05-26 23:31:46|                  17625|
    |144301|2017-05-26 23:31:46|     0.0|          Apocalypto|Action, Adventure...|  2006-12-08|40dd7bf1f9|0006ea6b5c|2017-05-27 22:45:41|                  83635|
    |145323|2017-05-27 22:45:41|     0.0|Joshua: Teenager ...|         Documentary|  2017-01-20|4a138aeefc|0006ea6b5c|2017-06-02 22:51:18|                 518737|
    |150621|2017-06-02 22:51:18|  1182.0|         Lucid Dream|    Sci-Fi, Thriller|  2017-06-02|27b44a3183|0006ea6b5c|2017-06-02 23:11:00|                   1182|
    |150043|2017-06-02 23:11:00|     0.0|Stranger than Fic...|Comedy, Drama, Fa...|  2006-11-10|73183024a6|0006ea6b5c|2017-06-03 21:54:46|                  81826|
    |151464|2017-06-03 21:54:46|  4200.0|Handsome: A Netfl...|     Comedy, Mystery|  2017-05-05|9f2550ca52|0006ea6b5c|2017-06-03 23:04:46|                   4200|
    |150783|2017-06-03 23:04:46|     0.0|        Dragon Blade|Action, Adventure...|  2015-09-04|ed515d444e|0006ea6b5c|2017-06-04 23:28:04|                  87798|
    |151782|2017-06-04 23:28:04|  1800.0|        Dragon Blade|Action, Adventure...|  2015-09-04|ed515d444e|0006ea6b5c|2017-06-09 21:14:59|                 424015|
    |156021|2017-06-09 21:14:59|     0.0|        Shimmer Lake|Crime, Drama, Mys...|  2017-06-09|09a559f1ce|0006ea6b5c|2017-06-10 23:25:25|                  94226|
    |156307|2017-06-10 23:25:25|  4800.0| Absolutely Anything|      Comedy, Sci-Fi|  2017-05-12|1e7ac0d4d4|0006ea6b5c|2017-06-18 03:12:09|                 618404|
    |162033|2017-06-18 03:12:09|     0.0|     A Plastic Ocean|         Documentary|  2017-04-19|9ac5a606e0|0006ea6b5c|2017-06-19 21:42:14|                 153005|
    |162421|2017-06-19 21:42:14|  4831.0|    Bulletproof Monk|Action, Comedy, F...|  2003-04-16|42689f3587|0006ea6b5c|2017-06-29 00:05:28|                 786194|
    |170114|2017-06-29 00:05:28|     0.0|                Okja|Action, Adventure...|  2017-06-28|0b1cdb1a41|0006ea6b5c|               NULL|                   NULL|
    |715907|2019-06-09 23:28:55|     0.0|         Last Breath|         Documentary|  2019-04-05|d05f61119f|0007fc8621|2019-06-10 21:59:39|                  81044|
    |716523|2019-06-10 21:59:39|     0.0|         Last Breath|         Documentary|  2019-04-05|d05f61119f|0007fc8621|               NULL|                   NULL|
    |370084|2018-03-15 21:30:54|  7652.0|        Annihilation|Adventure, Drama,...|  2018-02-23|1f579d43c3|000800c223|               NULL|                   NULL|
    |382347|2018-03-30 08:11:47|     0.0|David Brent: Life...|       Comedy, Music|  2017-02-10|e391b33f60|0008d919a5|2018-03-31 23:36:52|                 141905|
    |382935|2018-03-31 23:36:52|     0.0|Captain America: ...|Action, Adventure...|  2014-04-04|5b27e079e9|0008d919a5|               NULL|                   NULL|
    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+-------------------+-----------------------+
    only showing top 20 rows
    


                                                                                    


```python
# 2
df_peliculas = df_peliculas.withColumn(
    "es_zapping", F.when(F.col("calculated_time_to_next") < 300, 1).otherwise(0)
)

df_peliculas.show(50)
```

    [Stage 115:=================================================>       (7 + 1) / 8]

    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+-------------------+-----------------------+----------+
    |row_id|           datetime|duration|               title|              genres|release_date|  movie_id|   user_id|      next_datetime|calculated_time_to_next|es_zapping|
    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+-------------------+-----------------------+----------+
    |139643|2017-05-19 20:21:43|     0.0|                XOXO|        Drama, Music|  2016-08-26|7369676dec|0006ea6b5c|2017-05-20 21:54:34|                  91971|         0|
    |140442|2017-05-20 21:54:34|     0.0|            Hot Fuzz|Action, Comedy, M...|  2007-04-20|6467fee6b6|0006ea6b5c|2017-05-26 18:38:01|                 506607|         0|
    |144717|2017-05-26 18:38:01|     0.0|         War Machine|  Comedy, Drama, War|  2017-05-26|0f3b137f4e|0006ea6b5c|2017-05-26 23:31:46|                  17625|         0|
    |144301|2017-05-26 23:31:46|     0.0|          Apocalypto|Action, Adventure...|  2006-12-08|40dd7bf1f9|0006ea6b5c|2017-05-27 22:45:41|                  83635|         0|
    |145323|2017-05-27 22:45:41|     0.0|Joshua: Teenager ...|         Documentary|  2017-01-20|4a138aeefc|0006ea6b5c|2017-06-02 22:51:18|                 518737|         0|
    |150621|2017-06-02 22:51:18|  1182.0|         Lucid Dream|    Sci-Fi, Thriller|  2017-06-02|27b44a3183|0006ea6b5c|2017-06-02 23:11:00|                   1182|         0|
    |150043|2017-06-02 23:11:00|     0.0|Stranger than Fic...|Comedy, Drama, Fa...|  2006-11-10|73183024a6|0006ea6b5c|2017-06-03 21:54:46|                  81826|         0|
    |151464|2017-06-03 21:54:46|  4200.0|Handsome: A Netfl...|     Comedy, Mystery|  2017-05-05|9f2550ca52|0006ea6b5c|2017-06-03 23:04:46|                   4200|         0|
    |150783|2017-06-03 23:04:46|     0.0|        Dragon Blade|Action, Adventure...|  2015-09-04|ed515d444e|0006ea6b5c|2017-06-04 23:28:04|                  87798|         0|
    |151782|2017-06-04 23:28:04|  1800.0|        Dragon Blade|Action, Adventure...|  2015-09-04|ed515d444e|0006ea6b5c|2017-06-09 21:14:59|                 424015|         0|
    |156021|2017-06-09 21:14:59|     0.0|        Shimmer Lake|Crime, Drama, Mys...|  2017-06-09|09a559f1ce|0006ea6b5c|2017-06-10 23:25:25|                  94226|         0|
    |156307|2017-06-10 23:25:25|  4800.0| Absolutely Anything|      Comedy, Sci-Fi|  2017-05-12|1e7ac0d4d4|0006ea6b5c|2017-06-18 03:12:09|                 618404|         0|
    |162033|2017-06-18 03:12:09|     0.0|     A Plastic Ocean|         Documentary|  2017-04-19|9ac5a606e0|0006ea6b5c|2017-06-19 21:42:14|                 153005|         0|
    |162421|2017-06-19 21:42:14|  4831.0|    Bulletproof Monk|Action, Comedy, F...|  2003-04-16|42689f3587|0006ea6b5c|2017-06-29 00:05:28|                 786194|         0|
    |170114|2017-06-29 00:05:28|     0.0|                Okja|Action, Adventure...|  2017-06-28|0b1cdb1a41|0006ea6b5c|               NULL|                   NULL|         0|
    |715907|2019-06-09 23:28:55|     0.0|         Last Breath|         Documentary|  2019-04-05|d05f61119f|0007fc8621|2019-06-10 21:59:39|                  81044|         0|
    |716523|2019-06-10 21:59:39|     0.0|         Last Breath|         Documentary|  2019-04-05|d05f61119f|0007fc8621|               NULL|                   NULL|         0|
    |370084|2018-03-15 21:30:54|  7652.0|        Annihilation|Adventure, Drama,...|  2018-02-23|1f579d43c3|000800c223|               NULL|                   NULL|         0|
    |382347|2018-03-30 08:11:47|     0.0|David Brent: Life...|       Comedy, Music|  2017-02-10|e391b33f60|0008d919a5|2018-03-31 23:36:52|                 141905|         0|
    |382935|2018-03-31 23:36:52|     0.0|Captain America: ...|Action, Adventure...|  2014-04-04|5b27e079e9|0008d919a5|               NULL|                   NULL|         0|
    |242183|2017-10-07 12:52:17|   593.0|         Paper Towns|Comedy, Drama, My...|  2015-07-24|32f163e302|000b4a3b02|               NULL|                   NULL|         0|
    |316253|2018-01-13 13:18:19|     0.0|    The Big Lebowski|Comedy, Crime, Sport|  1998-03-06|d601124c11|0012292e7f|               NULL|                   NULL|         0|
    |382733|2018-03-31 22:41:05|   133.0|     Take Your Pills|       NOT AVAILABLE|        NULL|2b9b022a4a|00137fc554|               NULL|                   NULL|         0|
    |716156|2019-06-09 01:26:30|   636.0|      Murder Mystery|       NOT AVAILABLE|        NULL|e28584bcbe|0014af417e|2019-06-09 01:37:06|                    636|         0|
    |716273|2019-06-09 01:37:06|124148.0|      Murder Mystery|       NOT AVAILABLE|        NULL|e28584bcbe|0014af417e|               NULL|                   NULL|         0|
    |390577|2018-04-08 03:04:28|  7829.0|Valerian and the ...|Action, Adventure...|  2017-07-21|ab54ae1c45|00176a8729|2018-04-22 04:03:19|                1213131|         0|
    |400688|2018-04-22 04:03:19| 64804.0|Alice Through the...|Adventure, Family...|  2016-05-27|50f112410a|00176a8729|2018-05-01 13:41:26|                 812287|         0|
    |407594|2018-05-01 13:41:26|  9707.0|       Before I Wake|Drama, Fantasy, H...|  2018-01-05|df63006bd8|00176a8729|2018-05-01 21:04:31|                  26585|         0|
    |407348|2018-05-01 21:04:31|  6181.0|             Krampus|Comedy, Drama, Fa...|  2015-12-04|2fadcf80ca|00176a8729|               NULL|                   NULL|         0|
    |198526|2017-08-06 06:29:51|     0.0|  Enemy at the Gates| Drama, History, War|  2001-03-16|dda0eae17b|00184fcc7f|2017-08-06 06:29:51|                      0|         1|
    |198549|2017-08-06 06:29:51|     0.0|  Enemy at the Gates| Drama, History, War|  2001-03-16|dda0eae17b|00184fcc7f|2017-08-06 07:41:48|                   4317|         0|
    |198756|2017-08-06 07:41:48|     0.0|Hunt for the Wild...|Adventure, Comedy...|  2016-07-01|12f4b72e92|00184fcc7f|2017-08-06 16:01:41|                  29993|         0|
    |198314|2017-08-06 16:01:41|     0.0|               50/50|Comedy, Drama, Ro...|  2011-09-30|172b3f240d|00184fcc7f|               NULL|                   NULL|         0|
    | 61493|2017-01-06 09:58:34|  1906.0|Star Trek: First ...|Action, Adventure...|  1996-11-22|dfd60c5a87|001991be8a|2017-01-06 10:30:20|                   1906|         0|
    | 61509|2017-01-06 10:30:20|    46.0|Star Trek: First ...|Action, Adventure...|  1996-11-22|dfd60c5a87|001991be8a|               NULL|                   NULL|         0|
    |436171|2018-06-11 16:27:03|  1800.0|  Jeepers Creepers 3|Action, Horror, M...|  2017-11-02|9d162c50cf|00273c3294|2018-06-11 16:57:03|                   1800|         0|
    |436215|2018-06-11 16:57:03|   600.0|  Jeepers Creepers 3|Action, Horror, M...|  2017-11-02|9d162c50cf|00273c3294|2018-06-11 17:07:03|                    600|         0|
    |436256|2018-06-11 17:07:03|   366.0|  Jeepers Creepers 3|Action, Horror, M...|  2017-11-02|9d162c50cf|00273c3294|               NULL|                   NULL|         0|
    |157642|2017-06-11 12:37:01|     0.0|     The Guest House|             Romance|  2012-09-04|268154e1d0|002aa52f69|2017-06-13 09:36:52|                 161991|         0|
    |158670|2017-06-13 09:36:52|     0.0|Anchorman: The Le...|              Comedy|  2013-12-18|f80b7002bb|002aa52f69|               NULL|                   NULL|         0|
    |714063|2019-06-07 16:56:46|     0.0|          White Girl|        Crime, Drama|  2016-12-01|f79b98f1f0|002e8bd418|2019-06-08 02:00:47|                  32641|         0|
    |715354|2019-06-08 02:00:47|     0.0|   What a Girl Wants|Comedy, Drama, Fa...|  2003-04-04|e7ccc441bc|002e8bd418|2019-06-08 13:10:47|                  40200|         0|
    |714713|2019-06-08 13:10:47|     0.0|          White Girl|        Crime, Drama|  2016-12-01|f79b98f1f0|002e8bd418|2019-06-08 23:50:54|                  38407|         0|
    |714636|2019-06-08 23:50:54|     0.0|Louis Theroux: Sa...|         Documentary|  2016-10-02|681b8e0fad|002e8bd418|2019-06-09 19:18:57|                  70083|         0|
    |715776|2019-06-09 19:18:57| 14233.0|    Teenage Cocktail|     Drama, Thriller|  2017-01-15|6daa09c340|002e8bd418|2019-06-10 01:47:03|                  23286|         0|
    |716430|2019-06-10 01:47:03|     0.0|            Superbad|              Comedy|  2007-08-17|85847e36b9|002e8bd418|2019-06-10 23:37:59|                  78656|         0|
    |716413|2019-06-10 23:37:59|     0.0|    Teenage Cocktail|     Drama, Thriller|  2017-01-15|6daa09c340|002e8bd418|2019-06-11 00:38:00|                   3601|         0|
    |717215|2019-06-11 00:38:00|  3599.0|                 Cam|Horror, Mystery, ...|  2018-11-16|5ac3430b18|002e8bd418|2019-06-11 01:58:00|                   4800|         0|
    |717316|2019-06-11 01:58:00| 27101.0|Abducted in Plain...|Documentary, Crim...|  2019-01-15|b08e01920a|002e8bd418|               NULL|                   NULL|         0|
    |387125|2018-04-04 03:32:14|    11.0|        Kill Command|Action, Horror, S...|  2016-11-25|8db5e0d8dd|0034d5ca8e|2018-04-04 03:32:48|                     34|         1|
    +------+-------------------+--------+--------------------+--------------------+------------+----------+----------+-------------------+-----------------------+----------+
    only showing top 50 rows
    


                                                                                    


```python
# 3
df_peliculas = df_peliculas.withColumn(
    "fecha", F.to_date("datetime")
)

ventana = Window.partitionBy("user_id", "fecha").orderBy("datetime")

df_peliculas = df_peliculas.withColumn(
    "pelicula_nro", F.row_number().over(ventana)
)

df_peliculas.groupBy("user_id", "fecha") \
  .agg(F.max("pelicula_nro").alias("peliculas_en_el_dia")) \
  .filter(F.col("peliculas_en_el_dia") > 5) \
  .orderBy(F.col("peliculas_en_el_dia").desc()) \
  .show()
```

    [Stage 118:==============>                                          (2 + 6) / 8]

    +----------+----------+-------------------+
    |   user_id|     fecha|peliculas_en_el_dia|
    +----------+----------+-------------------+
    |23c52f9b50|2019-01-21|                 64|
    |59416738c3|2017-02-21|                 54|
    |3675d9ba4a|2018-11-26|                 44|
    |b15926c011|2018-03-24|                 42|
    |b090d94e51|2017-05-01|                 40|
    |423d95651d|2019-05-04|                 38|
    |fa48fa50ae|2018-12-22|                 38|
    |16d994f6dd|2017-11-11|                 37|
    |b15926c011|2018-05-13|                 36|
    |b15926c011|2018-04-15|                 36|
    |779343a3ea|2018-06-17|                 34|
    |f0d52ca74c|2018-03-30|                 34|
    |b15926c011|2018-05-06|                 33|
    |779343a3ea|2018-05-26|                 32|
    |6924354498|2017-11-05|                 31|
    |da01959c0b|2019-03-21|                 31|
    |5495affecb|2017-09-09|                 29|
    |f98c8ca112|2017-09-06|                 29|
    |da01959c0b|2019-03-12|                 29|
    |ae77c5157f|2017-02-21|                 29|
    +----------+----------+-------------------+
    only showing top 20 rows
    


                                                                                    


```python
# 4
ventana = Window.partitionBy("user_id", "title")

df_peliculas = df_peliculas.withColumn(
    "views_by_user", F.count("title").over(ventana)
)

df_reviews = df_peliculas.filter(F.col("views_by_user") >= 3) \
                    .orderBy("views_by_user", ascending=False) \
                    .select("user_id", "title", "views_by_user") \
                    .distinct()

df_reviews.show()
```

    [Stage 121:=======>                                                 (1 + 7) / 8]

    +----------+--------------------+-------------+
    |   user_id|               title|views_by_user|
    +----------+--------------------+-------------+
    |000052a0a0|              Looper|            9|
    |0012a95d5f|           Footloose|            3|
    |0016c962c8|          Iron Man 3|            4|
    |0023e9b95e|Black Mirror: Ban...|            3|
    |00305e5c73|                Lion|            4|
    |004ad258d2|              Carrie|            4|
    |004e33f215|             Detroit|            3|
    |004e33f215|   Hitler - A Career|            4|
    |004e33f215|The Legend of Coc...|            3|
    |00691f60a9|The Ballad of Bus...|            6|
    |006c65ab91| Fifty Shades Darker|            3|
    |007be0a9ea|           Set It Up|            3|
    |00870a4069|         Rush Hour 2|            3|
    |00b88e6abe|            Next Gen|            4|
    |00cbeebb4a|        Flushed Away|            3|
    |00e3a62319|               Close|            3|
    |00e4d08f81|Transformer: The ...|            3|
    |00fe57b583|Mowgli: Legend of...|            3|
    |00ffcbb5de|    Bad Neighbours 2|            3|
    |0125155c04|Mechanic: Resurre...|            3|
    +----------+--------------------+-------------+
    only showing top 20 rows
    


                                                                                    
