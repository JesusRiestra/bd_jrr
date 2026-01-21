```python
%%writefile mapper_1_patrones.py

# Ejercicio 1

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    if not linea:
        continue
        
    columnas = linea.strip().split(";")

    if columnas[0] == "country_code" or len(columnas) < 10:
        continue
        
    country = columnas[4]
    year = columnas[6]
    total_gdp = columnas[7]
    
    year = int(year)
    total_gdp = float(total_gdp)

    if year >= 2000 and total_gdp > 0:
        print(f"{country}\t{year}\t{total_gdp}")
```

    Overwriting mapper_1_patrones.py



```python
%%writefile reducer_1_patrones.py

# Ejercicio 1

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    print(linea.strip())
```

    Overwriting reducer_1_patrones.py



```python
%%writefile mapper_2_patrones.py

# Ejercicio 2

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split(";")

    if columnas[0] == "country_code":
        continue
        
    region = columnas[1]
    total_gdp = columnas[7]

    print(f"{region}\t{total_gdp}")
```

    Overwriting mapper_2_patrones.py



```python
%%writefile reducer_2_patrones.py

# Ejercicio 2

#!/usr/bin/env python3

import sys

region_actual = None
total = 0
contador = 0

for linea in sys.stdin:
    region, valor = linea.strip().split("\t")
    valor = float(valor)

    if region != region_actual:
        if region_actual is not None:
            promedio = total / contador
            print(f"{region_actual}\t{promedio}")
            
        region_actual = region
        total = valor
        contador = 1
    else:
        total += valor
        contador += 1

if region_actual is not None:
    promedio = total / contador
    print(f"{region_actual}\t{promedio}")
```

    Overwriting reducer_2_patrones.py



```python
%%writefile mapper_3_patrones.py

# Ejercicio 3

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split(";")

    if columnas[0] == "country_code":
        continue
        
    country = columnas[4]
    year = columnas[6]
    gdp_variation = columnas[9]

    print(f"{country}\t{year},{gdp_variation}")
```

    Overwriting mapper_3_patrones.py



```python
%%writefile reducer_3_patrones.py

# Ejercicio 3

#!/usr/bin/env python3

import sys

pais_actual = None
variacion_maxima = 0
anio_asociado = 0

for linea in sys.stdin:
    pais, valor = linea.strip().split("\t")
    anio, variacion = valor.split(",")
    
    variacion = float(variacion)
    
    if pais == pais_actual:
        if variacion > variacion_maxima:
            variacion_maxima = variacion
            anio_asociado = anio 
    else:
        if pais_actual:
            print(f"{pais_actual}\t{anio_asociado} ({variacion_maxima})")
        pais_actual = pais
        variacion_maxima = 0
        anio_asociado = 0

if pais_actual:
    print(f"{pais_actual}\t{anio_asociado} ({variacion_maxima})")
```

    Overwriting reducer_3_patrones.py



```python
!cat countries_gdp_hist.csv | python3 mapper_3_patrones.py | sort | python3 reducer_3_patrones.py
```


```python
%%writefile mapper_4_patrones.py

# Ejercicio 4

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()

    columnas_coma = linea.split(",")

    if ';' in linea:
        columnas = linea.split(';')
        
        if columnas[0].lower() == "country_code": 
            continue

        country_code = columnas[0]
        total_gdp = columnas[7]
            
        print(f"{country_code}\tB_{total_gdp}")

    else:
        columnas = linea.split(',')

        if columnas[0].lower() == "name": 
            continue

        country_name = columnas[0]
        codigo_pais = columnas[2]

        print(f"{codigo_pais}\tA_{country_name}")
```

    Overwriting mapper_4_patrones.py



```python
!cat countries_gdp_hist.csv | python3 mapper_4_patrones.py
```


```python
%%writefile reducer_4_patrones.py

# Ejercicio 4

#!/usr/bin/env python3

import sys

codigo_actual = None
nombre_pais = None

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue

    codigo_pais, valor = linea.split('\t', 1)
    etiqueta, datos = valor.split('_', 1)

    if codigo_pais != codigo_actual:
        codigo_actual = codigo_pais
        nombre_pais = None

    if etiqueta == "A":
        nombre_pais = datos
    elif etiqueta == "B" and nombre_pais:
        print(f"{nombre_pais}\t{datos}")
```

    Overwriting reducer_4_patrones.py



```python
%%writefile mapper_5_patrones.py

# Ejercicio 5

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue
    
    columnas = linea.strip().split(";")

    if columnas[0] == "country_code" or len(columnas) < 10:
        continue
        
    try:
        total_gdp_million = float(columnas[8])

        if total_gdp_million < 10000:
            print("Economía Pequeña\t1")
        elif total_gdp_million < 1000000:
            print("Economía Mediana\t1")
        else:
            print("Economía Grande\t1")
            
    except ValueError:
        continue
```

    Overwriting mapper_5_patrones.py



```python
%%writefile reducer_5_patrones.py

# Ejercicio 5

#!/usr/bin/env python3

import sys

categoria_actual = None
contador = 0

for linea in sys.stdin:
    linea = linea.strip()
    categoria, valor = linea.split('\t')

    if categoria != categoria_actual:
        if categoria_actual is not None:
            print(f"{categoria_actual}\t{contador}")
        categoria_actual = categoria
        contador = 0

    contador += int(valor)

if categoria_actual is not None:
    print(f"{categoria_actual}\t{contador}")
```

    Overwriting reducer_5_patrones.py



```python
%%writefile mapper_6_patrones.py

# Ejercicio 6

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue
    
    columnas = linea.strip().split(";")

    if columnas[0] == "country_code" or len(columnas) < 10:
        continue
        
    income_group = columnas[5]
    country = columnas[4]

    print(f"{income_group}\t{country}")
```

    Overwriting mapper_6_patrones.py



```python
%%writefile reducer_6_patrones.py

# Ejercicio 6

#!/usr/bin/env python3

import sys

grupo_actual = None
paises = set()

for linea in sys.stdin:
    linea = linea.strip()
    income_group, country = linea.split('\t', 1)

    if income_group != grupo_actual:
        if grupo_actual is not None:
            print(f"{grupo_actual}\t{', '.join(sorted(paises))}")
        grupo_actual = income_group
        paises = set()

    paises.add(country)

if grupo_actual is not None:
    print(f"{grupo_actual}\t{', '.join(sorted(paises))}")
```

    Overwriting reducer_6_patrones.py

