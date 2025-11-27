```python
import redis
```


```python
POIS = [
    {"id": "poi_001", "name": "Puerta del Sol", "lon": -3.703790, "lat": 40.416775},
    {"id": "poi_002", "name": "Museo del Prado", "lon": -3.692140, "lat": 40.413780},
    {"id": "poi_003", "name": "Parque del Retiro", "lon": -3.684440, "lat": 40.415360},
    {"id": "poi_004", "name": "Palacio Real", "lon": -3.714310, "lat": 40.417910},
    {"id": "poi_005", "name": "Plaza Mayor", "lon": -3.707370, "lat": 40.415380},
    {"id": "poi_006", "name": "Museo Reina Sofía", "lon": -3.694340, "lat": 40.408010},
    {"id": "poi_007", "name": "Museo Thyssen-Bornemisza", "lon": -3.695000, "lat": 40.416100},
    {"id": "poi_008", "name": "Estadio Santiago Bernabéu", "lon": -3.692380, "lat": 40.453050},
    {"id": "poi_009", "name": "Gran Vía (Plaza Callao)", "lon": -3.708000, "lat": 40.420200},
    {"id": "poi_010", "name": "Templo de Debod", "lon": -3.718000, "lat": 40.424300},
    {"id": "poi_011", "name": "Mercado de San Miguel", "lon": -3.709300, "lat": 40.415000},
    {"id": "poi_012", "name": "Catedral de la Almudena", "lon": -3.714200, "lat": 40.416000},
    {"id": "poi_013", "name": "Estación de Atocha", "lon": -3.690500, "lat": 40.406900},
    {"id": "poi_014", "name": "Plaza de Cibeles", "lon": -3.693000, "lat": 40.419200},
    {"id": "poi_015", "name": "Puerta de Alcalá", "lon": -3.688700, "lat": 40.420500},
    {"id": "poi_016", "name": "Plaza de España", "lon": -3.712000, "lat": 40.423900},
    {"id": "poi_017", "name": "CaixaForum Madrid", "lon": -3.693400, "lat": 40.409300},
    {"id": "poi_018", "name": "Plaza de Cascorro (El Rastro)", "lon": -3.706700, "lat": 40.411100},
    {"id": "poi_019", "name": "Matadero Madrid", "lon": -3.703200, "lat": 40.391600},
    {"id": "poi_020", "name": "Estadio Cívitas Metropolitano", "lon": -3.599100, "lat": 40.436300},
]
```


```python
# Tarea 1: Carga de datos

def load_locations():
    r = redis.Redis(
    host="redis",
    port=6379,
    db=0,
    decode_responses=True
    )
    
    for poi in POIS:
        r.geoadd("poi:locations", [poi['lon'], poi['lat'], poi['id']])
        r.hset("poi:info", poi['id'], poi['name'])

    print("Datos cargados!")
```


```python
load_locations()
```

    Datos cargados!



```python
# Tarea 2: Búsqueda por radio

def find_by_radius(lat, lon, distance=2000):
    r = redis.Redis(
    host="redis",
    port=6379,
    db=0,
    decode_responses=True
    )
    
    poi_ids = r.geosearch(
        name = "poi:locations",
        longitude = lon,
        latitude = lat,
        radius = distance,
        unit = "m"
    )

    nombres_poi = r.hmget("poi:info", poi_ids)

    print(f"Encontrados {len(poi_ids)} POIs en {distance/1000:.1f} km:")
    for poi_id, nombre_poi in zip(poi_ids, nombres_poi):
        print(f"-> {nombre_poi} ({poi_id})")
```


```python
find_by_radius(40.41677, -3.70379, 1000)
```

    Encontrados 9 POIs en 1.0 km:
    -> Plaza de Cascorro (El Rastro) (poi_018)
    -> Mercado de San Miguel (poi_011)
    -> Plaza Mayor (poi_005)
    -> Puerta del Sol (poi_001)
    -> Museo Thyssen-Bornemisza (poi_007)
    -> Gran Vía (Plaza Callao) (poi_009)
    -> Plaza de Cibeles (poi_014)
    -> Catedral de la Almudena (poi_012)
    -> Palacio Real (poi_004)

