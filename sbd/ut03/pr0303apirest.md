```python
import requests
import pandas as pd
```


```python
# 1.1
url = "https://swapi.dev/api/vehicles/"

response = requests.get(url)

if response.status_code == 200:
    print("¡Conexión establecida!")
else:
    print(f"Error: {response.status_code}")
```

    ¡Conexión establecida!



```python
# 1.2
datos = response.json()
results = datos["results"]
```


```python
# 1.3
df_vehicles = pd.DataFrame(results)
```


```python
# 1.4
df_vehicles.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>model</th>
      <th>manufacturer</th>
      <th>cost_in_credits</th>
      <th>length</th>
      <th>max_atmosphering_speed</th>
      <th>crew</th>
      <th>passengers</th>
      <th>cargo_capacity</th>
      <th>consumables</th>
      <th>vehicle_class</th>
      <th>pilots</th>
      <th>films</th>
      <th>created</th>
      <th>edited</th>
      <th>url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sand Crawler</td>
      <td>Digger Crawler</td>
      <td>Corellia Mining Corporation</td>
      <td>150000</td>
      <td>36.8</td>
      <td>30</td>
      <td>46</td>
      <td>30</td>
      <td>50000</td>
      <td>2 months</td>
      <td>wheeled</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T15:36:25.724000Z</td>
      <td>2014-12-20T21:30:21.661000Z</td>
      <td>https://swapi.dev/api/vehicles/4/</td>
    </tr>
    <tr>
      <th>1</th>
      <td>T-16 skyhopper</td>
      <td>T-16 skyhopper</td>
      <td>Incom Corporation</td>
      <td>14500</td>
      <td>10.4</td>
      <td>1200</td>
      <td>1</td>
      <td>1</td>
      <td>50</td>
      <td>0</td>
      <td>repulsorcraft</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>2014-12-10T16:01:52.434000Z</td>
      <td>2014-12-20T21:30:21.665000Z</td>
      <td>https://swapi.dev/api/vehicles/6/</td>
    </tr>
    <tr>
      <th>2</th>
      <td>X-34 landspeeder</td>
      <td>X-34 landspeeder</td>
      <td>SoroSuub Corporation</td>
      <td>10550</td>
      <td>3.4</td>
      <td>250</td>
      <td>1</td>
      <td>1</td>
      <td>5</td>
      <td>unknown</td>
      <td>repulsorcraft</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>2014-12-10T16:13:52.586000Z</td>
      <td>2014-12-20T21:30:21.668000Z</td>
      <td>https://swapi.dev/api/vehicles/7/</td>
    </tr>
    <tr>
      <th>3</th>
      <td>TIE/LN starfighter</td>
      <td>Twin Ion Engine/Ln Starfighter</td>
      <td>Sienar Fleet Systems</td>
      <td>unknown</td>
      <td>6.4</td>
      <td>1200</td>
      <td>1</td>
      <td>0</td>
      <td>65</td>
      <td>2 days</td>
      <td>starfighter</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T16:33:52.860000Z</td>
      <td>2014-12-20T21:30:21.670000Z</td>
      <td>https://swapi.dev/api/vehicles/8/</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Snowspeeder</td>
      <td>t-47 airspeeder</td>
      <td>Incom corporation</td>
      <td>unknown</td>
      <td>4.5</td>
      <td>650</td>
      <td>2</td>
      <td>0</td>
      <td>10</td>
      <td>none</td>
      <td>airspeeder</td>
      <td>[https://swapi.dev/api/people/1/, https://swap...</td>
      <td>[https://swapi.dev/api/films/2/]</td>
      <td>2014-12-15T12:22:12Z</td>
      <td>2014-12-20T21:30:21.672000Z</td>
      <td>https://swapi.dev/api/vehicles/14/</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_vehicles.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 10 entries, 0 to 9
    Data columns (total 16 columns):
     #   Column                  Non-Null Count  Dtype 
    ---  ------                  --------------  ----- 
     0   name                    10 non-null     object
     1   model                   10 non-null     object
     2   manufacturer            10 non-null     object
     3   cost_in_credits         10 non-null     object
     4   length                  10 non-null     object
     5   max_atmosphering_speed  10 non-null     object
     6   crew                    10 non-null     object
     7   passengers              10 non-null     object
     8   cargo_capacity          10 non-null     object
     9   consumables             10 non-null     object
     10  vehicle_class           10 non-null     object
     11  pilots                  10 non-null     object
     12  films                   10 non-null     object
     13  created                 10 non-null     object
     14  edited                  10 non-null     object
     15  url                     10 non-null     object
    dtypes: object(16)
    memory usage: 1.4+ KB



```python
# 2.1
url = "https://swapi.dev/api/people/"
people = []
total_count = 0

while url:
    try:
        response = requests.get(url)

        if response.status_code == 200:
            datos = response.json()
            total_count = datos["count"]
            people.extend(datos["results"])
            url = datos["next"]
            print("¡Página procesada!")
        else:
            print("Error de petición")
            break
            
    except Exception as e:
        print(f"Error: {e}")

# 2.3
df_people = pd.DataFrame(people)
```

    ¡Página procesada!
    ¡Página procesada!
    ¡Página procesada!
    ¡Página procesada!
    ¡Página procesada!
    ¡Página procesada!
    ¡Página procesada!
    ¡Página procesada!
    ¡Página procesada!



```python
# 2.4
df_people
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>height</th>
      <th>mass</th>
      <th>hair_color</th>
      <th>skin_color</th>
      <th>eye_color</th>
      <th>birth_year</th>
      <th>gender</th>
      <th>homeworld</th>
      <th>films</th>
      <th>species</th>
      <th>vehicles</th>
      <th>starships</th>
      <th>created</th>
      <th>edited</th>
      <th>url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Luke Skywalker</td>
      <td>172</td>
      <td>77</td>
      <td>blond</td>
      <td>fair</td>
      <td>blue</td>
      <td>19BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/vehicles/14/, https://s...</td>
      <td>[https://swapi.dev/api/starships/12/, https://...</td>
      <td>2014-12-09T13:50:51.644000Z</td>
      <td>2014-12-20T21:17:56.891000Z</td>
      <td>https://swapi.dev/api/people/1/</td>
    </tr>
    <tr>
      <th>1</th>
      <td>C-3PO</td>
      <td>167</td>
      <td>75</td>
      <td>n/a</td>
      <td>gold</td>
      <td>yellow</td>
      <td>112BBY</td>
      <td>n/a</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[https://swapi.dev/api/species/2/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T15:10:51.357000Z</td>
      <td>2014-12-20T21:17:50.309000Z</td>
      <td>https://swapi.dev/api/people/2/</td>
    </tr>
    <tr>
      <th>2</th>
      <td>R2-D2</td>
      <td>96</td>
      <td>32</td>
      <td>n/a</td>
      <td>white, blue</td>
      <td>red</td>
      <td>33BBY</td>
      <td>n/a</td>
      <td>https://swapi.dev/api/planets/8/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[https://swapi.dev/api/species/2/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T15:11:50.376000Z</td>
      <td>2014-12-20T21:17:50.311000Z</td>
      <td>https://swapi.dev/api/people/3/</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Darth Vader</td>
      <td>202</td>
      <td>136</td>
      <td>none</td>
      <td>white</td>
      <td>yellow</td>
      <td>41.9BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/starships/13/]</td>
      <td>2014-12-10T15:18:20.704000Z</td>
      <td>2014-12-20T21:17:50.313000Z</td>
      <td>https://swapi.dev/api/people/4/</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Leia Organa</td>
      <td>150</td>
      <td>49</td>
      <td>brown</td>
      <td>light</td>
      <td>brown</td>
      <td>19BBY</td>
      <td>female</td>
      <td>https://swapi.dev/api/planets/2/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/vehicles/30/]</td>
      <td>[]</td>
      <td>2014-12-10T15:20:09.791000Z</td>
      <td>2014-12-20T21:17:50.315000Z</td>
      <td>https://swapi.dev/api/people/5/</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>77</th>
      <td>Grievous</td>
      <td>216</td>
      <td>159</td>
      <td>none</td>
      <td>brown, white</td>
      <td>green, yellow</td>
      <td>unknown</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/59/</td>
      <td>[https://swapi.dev/api/films/6/]</td>
      <td>[https://swapi.dev/api/species/36/]</td>
      <td>[https://swapi.dev/api/vehicles/60/]</td>
      <td>[https://swapi.dev/api/starships/74/]</td>
      <td>2014-12-20T19:43:53.348000Z</td>
      <td>2014-12-20T21:17:50.488000Z</td>
      <td>https://swapi.dev/api/people/79/</td>
    </tr>
    <tr>
      <th>78</th>
      <td>Tarfful</td>
      <td>234</td>
      <td>136</td>
      <td>brown</td>
      <td>brown</td>
      <td>blue</td>
      <td>unknown</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/14/</td>
      <td>[https://swapi.dev/api/films/6/]</td>
      <td>[https://swapi.dev/api/species/3/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-20T19:46:34.209000Z</td>
      <td>2014-12-20T21:17:50.491000Z</td>
      <td>https://swapi.dev/api/people/80/</td>
    </tr>
    <tr>
      <th>79</th>
      <td>Raymus Antilles</td>
      <td>188</td>
      <td>79</td>
      <td>brown</td>
      <td>light</td>
      <td>brown</td>
      <td>unknown</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/2/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-20T19:49:35.583000Z</td>
      <td>2014-12-20T21:17:50.493000Z</td>
      <td>https://swapi.dev/api/people/81/</td>
    </tr>
    <tr>
      <th>80</th>
      <td>Sly Moore</td>
      <td>178</td>
      <td>48</td>
      <td>none</td>
      <td>pale</td>
      <td>white</td>
      <td>unknown</td>
      <td>female</td>
      <td>https://swapi.dev/api/planets/60/</td>
      <td>[https://swapi.dev/api/films/5/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-20T20:18:37.619000Z</td>
      <td>2014-12-20T21:17:50.496000Z</td>
      <td>https://swapi.dev/api/people/82/</td>
    </tr>
    <tr>
      <th>81</th>
      <td>Tion Medon</td>
      <td>206</td>
      <td>80</td>
      <td>none</td>
      <td>grey</td>
      <td>black</td>
      <td>unknown</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/12/</td>
      <td>[https://swapi.dev/api/films/6/]</td>
      <td>[https://swapi.dev/api/species/37/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-20T20:35:04.260000Z</td>
      <td>2014-12-20T21:17:50.498000Z</td>
      <td>https://swapi.dev/api/people/83/</td>
    </tr>
  </tbody>
</table>
<p>82 rows × 16 columns</p>
</div>




```python
df_reducido = df_people.head(20).copy()

def info_planeta(url):
    try:
        response = requests.get(url)

        if response.status_code == 200:
            datos = response.json()
            return pd.Series({
                "planet_name": datos.get("name"), 
                "planet_terrain": datos.get("terrain"), 
                "planet_population": datos.get("population")
            })
        else:
            print("Error de petición")
            return pd.Series([None, None, None])
    except Exception as e:
        print(f"Error: {e}")
        return pd.Series([None, None, None])
        
df_reducido[["planet_name", "planet_terrain", "planet_population"]] = df_reducido["homeworld"].apply(info_planeta)

df_reducido
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>height</th>
      <th>mass</th>
      <th>hair_color</th>
      <th>skin_color</th>
      <th>eye_color</th>
      <th>birth_year</th>
      <th>gender</th>
      <th>homeworld</th>
      <th>films</th>
      <th>species</th>
      <th>vehicles</th>
      <th>starships</th>
      <th>created</th>
      <th>edited</th>
      <th>url</th>
      <th>planet_name</th>
      <th>planet_terrain</th>
      <th>planet_population</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Luke Skywalker</td>
      <td>172</td>
      <td>77</td>
      <td>blond</td>
      <td>fair</td>
      <td>blue</td>
      <td>19BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/vehicles/14/, https://s...</td>
      <td>[https://swapi.dev/api/starships/12/, https://...</td>
      <td>2014-12-09T13:50:51.644000Z</td>
      <td>2014-12-20T21:17:56.891000Z</td>
      <td>https://swapi.dev/api/people/1/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>C-3PO</td>
      <td>167</td>
      <td>75</td>
      <td>n/a</td>
      <td>gold</td>
      <td>yellow</td>
      <td>112BBY</td>
      <td>n/a</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[https://swapi.dev/api/species/2/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T15:10:51.357000Z</td>
      <td>2014-12-20T21:17:50.309000Z</td>
      <td>https://swapi.dev/api/people/2/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>R2-D2</td>
      <td>96</td>
      <td>32</td>
      <td>n/a</td>
      <td>white, blue</td>
      <td>red</td>
      <td>33BBY</td>
      <td>n/a</td>
      <td>https://swapi.dev/api/planets/8/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[https://swapi.dev/api/species/2/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T15:11:50.376000Z</td>
      <td>2014-12-20T21:17:50.311000Z</td>
      <td>https://swapi.dev/api/people/3/</td>
      <td>Naboo</td>
      <td>grassy hills, swamps, forests, mountains</td>
      <td>4500000000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Darth Vader</td>
      <td>202</td>
      <td>136</td>
      <td>none</td>
      <td>white</td>
      <td>yellow</td>
      <td>41.9BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/starships/13/]</td>
      <td>2014-12-10T15:18:20.704000Z</td>
      <td>2014-12-20T21:17:50.313000Z</td>
      <td>https://swapi.dev/api/people/4/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Leia Organa</td>
      <td>150</td>
      <td>49</td>
      <td>brown</td>
      <td>light</td>
      <td>brown</td>
      <td>19BBY</td>
      <td>female</td>
      <td>https://swapi.dev/api/planets/2/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/vehicles/30/]</td>
      <td>[]</td>
      <td>2014-12-10T15:20:09.791000Z</td>
      <td>2014-12-20T21:17:50.315000Z</td>
      <td>https://swapi.dev/api/people/5/</td>
      <td>Alderaan</td>
      <td>grasslands, mountains</td>
      <td>2000000000</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Owen Lars</td>
      <td>178</td>
      <td>120</td>
      <td>brown, grey</td>
      <td>light</td>
      <td>blue</td>
      <td>52BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T15:52:14.024000Z</td>
      <td>2014-12-20T21:17:50.317000Z</td>
      <td>https://swapi.dev/api/people/6/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Beru Whitesun lars</td>
      <td>165</td>
      <td>75</td>
      <td>brown</td>
      <td>light</td>
      <td>blue</td>
      <td>47BBY</td>
      <td>female</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T15:53:41.121000Z</td>
      <td>2014-12-20T21:17:50.319000Z</td>
      <td>https://swapi.dev/api/people/7/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>7</th>
      <td>R5-D4</td>
      <td>97</td>
      <td>32</td>
      <td>n/a</td>
      <td>white, red</td>
      <td>red</td>
      <td>unknown</td>
      <td>n/a</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>[https://swapi.dev/api/species/2/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T15:57:50.959000Z</td>
      <td>2014-12-20T21:17:50.321000Z</td>
      <td>https://swapi.dev/api/people/8/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Biggs Darklighter</td>
      <td>183</td>
      <td>84</td>
      <td>black</td>
      <td>light</td>
      <td>brown</td>
      <td>24BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/starships/12/]</td>
      <td>2014-12-10T15:59:50.509000Z</td>
      <td>2014-12-20T21:17:50.323000Z</td>
      <td>https://swapi.dev/api/people/9/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Obi-Wan Kenobi</td>
      <td>182</td>
      <td>77</td>
      <td>auburn, white</td>
      <td>fair</td>
      <td>blue-gray</td>
      <td>57BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/20/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/vehicles/38/]</td>
      <td>[https://swapi.dev/api/starships/48/, https://...</td>
      <td>2014-12-10T16:16:29.192000Z</td>
      <td>2014-12-20T21:17:50.325000Z</td>
      <td>https://swapi.dev/api/people/10/</td>
      <td>Stewjon</td>
      <td>grass</td>
      <td>unknown</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Anakin Skywalker</td>
      <td>188</td>
      <td>84</td>
      <td>blond</td>
      <td>fair</td>
      <td>blue</td>
      <td>41.9BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/1/</td>
      <td>[https://swapi.dev/api/films/4/, https://swapi...</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/vehicles/44/, https://s...</td>
      <td>[https://swapi.dev/api/starships/39/, https://...</td>
      <td>2014-12-10T16:20:44.310000Z</td>
      <td>2014-12-20T21:17:50.327000Z</td>
      <td>https://swapi.dev/api/people/11/</td>
      <td>Tatooine</td>
      <td>desert</td>
      <td>200000</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Wilhuff Tarkin</td>
      <td>180</td>
      <td>unknown</td>
      <td>auburn, grey</td>
      <td>fair</td>
      <td>blue</td>
      <td>64BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/21/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T16:26:56.138000Z</td>
      <td>2014-12-20T21:17:50.330000Z</td>
      <td>https://swapi.dev/api/people/12/</td>
      <td>Eriadu</td>
      <td>cityscape</td>
      <td>22000000000</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Chewbacca</td>
      <td>228</td>
      <td>112</td>
      <td>brown</td>
      <td>unknown</td>
      <td>blue</td>
      <td>200BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/14/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[https://swapi.dev/api/species/3/]</td>
      <td>[https://swapi.dev/api/vehicles/19/]</td>
      <td>[https://swapi.dev/api/starships/10/, https://...</td>
      <td>2014-12-10T16:42:45.066000Z</td>
      <td>2014-12-20T21:17:50.332000Z</td>
      <td>https://swapi.dev/api/people/13/</td>
      <td>Kashyyyk</td>
      <td>jungle, forests, lakes, rivers</td>
      <td>45000000</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Han Solo</td>
      <td>180</td>
      <td>80</td>
      <td>brown</td>
      <td>fair</td>
      <td>brown</td>
      <td>29BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/22/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/starships/10/, https://...</td>
      <td>2014-12-10T16:49:14.582000Z</td>
      <td>2014-12-20T21:17:50.334000Z</td>
      <td>https://swapi.dev/api/people/14/</td>
      <td>Corellia</td>
      <td>plains, urban, hills, forests</td>
      <td>3000000000</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Greedo</td>
      <td>173</td>
      <td>74</td>
      <td>n/a</td>
      <td>green</td>
      <td>black</td>
      <td>44BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/23/</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>[https://swapi.dev/api/species/4/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T17:03:30.334000Z</td>
      <td>2014-12-20T21:17:50.336000Z</td>
      <td>https://swapi.dev/api/people/15/</td>
      <td>Rodia</td>
      <td>jungles, oceans, urban, swamps</td>
      <td>1300000000</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Jabba Desilijic Tiure</td>
      <td>175</td>
      <td>1,358</td>
      <td>n/a</td>
      <td>green-tan, brown</td>
      <td>orange</td>
      <td>600BBY</td>
      <td>hermaphrodite</td>
      <td>https://swapi.dev/api/planets/24/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[https://swapi.dev/api/species/5/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-10T17:11:31.638000Z</td>
      <td>2014-12-20T21:17:50.338000Z</td>
      <td>https://swapi.dev/api/people/16/</td>
      <td>Nal Hutta</td>
      <td>urban, oceans, swamps, bogs</td>
      <td>7000000000</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Wedge Antilles</td>
      <td>170</td>
      <td>77</td>
      <td>brown</td>
      <td>fair</td>
      <td>hazel</td>
      <td>21BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/22/</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/vehicles/14/]</td>
      <td>[https://swapi.dev/api/starships/12/]</td>
      <td>2014-12-12T11:08:06.469000Z</td>
      <td>2014-12-20T21:17:50.341000Z</td>
      <td>https://swapi.dev/api/people/18/</td>
      <td>Corellia</td>
      <td>plains, urban, hills, forests</td>
      <td>3000000000</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Jek Tono Porkins</td>
      <td>180</td>
      <td>110</td>
      <td>brown</td>
      <td>fair</td>
      <td>blue</td>
      <td>unknown</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/26/</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/starships/12/]</td>
      <td>2014-12-12T11:16:56.569000Z</td>
      <td>2014-12-20T21:17:50.343000Z</td>
      <td>https://swapi.dev/api/people/19/</td>
      <td>Bestine IV</td>
      <td>rocky islands, oceans</td>
      <td>62000000</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Yoda</td>
      <td>66</td>
      <td>17</td>
      <td>white</td>
      <td>green</td>
      <td>brown</td>
      <td>896BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/28/</td>
      <td>[https://swapi.dev/api/films/2/, https://swapi...</td>
      <td>[https://swapi.dev/api/species/6/]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-15T12:26:01.042000Z</td>
      <td>2014-12-20T21:17:50.345000Z</td>
      <td>https://swapi.dev/api/people/20/</td>
      <td>unknown</td>
      <td>unknown</td>
      <td>unknown</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Palpatine</td>
      <td>170</td>
      <td>75</td>
      <td>grey</td>
      <td>pale</td>
      <td>yellow</td>
      <td>82BBY</td>
      <td>male</td>
      <td>https://swapi.dev/api/planets/8/</td>
      <td>[https://swapi.dev/api/films/2/, https://swapi...</td>
      <td>[]</td>
      <td>[]</td>
      <td>[]</td>
      <td>2014-12-15T12:48:05.971000Z</td>
      <td>2014-12-20T21:17:50.347000Z</td>
      <td>https://swapi.dev/api/people/21/</td>
      <td>Naboo</td>
      <td>grassy hills, swamps, forests, mountains</td>
      <td>4500000000</td>
    </tr>
  </tbody>
</table>
</div>


