```python
%%writefile mapper1.py

# Ejercicio 1 Mapper

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    columnas = linea.split(',')

    if (columnas[7] == "AvgTemperature"):
        continue
    
    region, country, state, city, month, day, year, temp = columnas

    try:
        temp = float(temp)
    except ValueError:
        continue
    
    print(f"{city}\t{(temp-32)/1.8}")
```

    Overwriting mapper1.py



```python
%%writefile reducer1.py

# Ejercicio 1 Reducer

#!/usr/bin/env python3

import sys

ciudad_actual = None
temperatura_max = None

for linea in sys.stdin:
    linea = linea.strip()
    ciudad, temp = linea.split('\t')

    try:
        temp = float(temp)
    except ValueError:
        continue

    if ciudad_actual is None:
        ciudad_actual = ciudad
        temperatura_max = temp

    elif ciudad == ciudad_actual:
        if temp > temperatura_max:
            temperatura_max = temp

    else:
        print(f"{ciudad_actual}\t{temperatura_max}")

        ciudad_actual = ciudad
        temperatura_max = temp

if ciudad_actual != None:
    print(f"{ciudad_actual}\t{temperatura_max}")
```

    Overwriting reducer1.py



```python
%%writefile mapper2.py

# Ejercicio 2 Mapper

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    columnas = linea.split(',')

    if (columnas[7] == "AvgTemperature"):
        continue
    
    region, country, state, city, month, day, year, temp = columnas

    try:
        temp = float(temp)
    except ValueError:
        continue
    
    print(f"{country}\t{(temp-32)/1.8}")
```

    Overwriting mapper2.py



```python
%%writefile reducer2.py

# Ejercicio 2 Reducer

#!/usr/bin/env python3

import sys

pais_actual = None
suma_temperaturas = 0
contador = 0

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue

    partes = linea.split('\t')
    if len(partes) != 2:
        continue

    pais, temp = partes
    
    try:
        temp = float(temp)
    except ValueError:
        continue

    if pais_actual is None:
        pais_actual = pais
        suma_temperaturas = temp
        contador = 1

    elif pais == pais_actual:
        suma_temperaturas += temp
        contador += 1

    else:
        media = suma_temperaturas / contador
        print(f"{pais_actual}\t{media:.2f}")

        pais_actual = pais
        suma_temperaturas = temp
        contador = 1

if pais_actual != None:
    media = suma_temperaturas / contador
    print(f"{pais_actual}\t{media:.2f}")
```

    Overwriting reducer2.py



```python
%%writefile mapper3.py

# Ejercicio 3 Mapper

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    columnas = linea.split(',')

    if columnas[7] == "AvgTemperature":
        continue

    region, country, state, city, month, day, year, temp = columnas

    try:
        temp = float(temp)
    except ValueError:
        continue

    temp_c = (temp - 32) / 1.8

    if temp_c > 30:
        print(f"{city},{year}\t1")

```

    Overwriting mapper3.py



```python
%%writefile reducer3.py

# Ejercicio 3 Reducer

#!/usr/bin/env python3

import sys

clave_actual = None
contador = 0

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue

    partes = linea.split('\t')
    if len(partes) != 2:
        continue

    clave, valor = partes
    try:
        valor = int(valor)
    except ValueError:
        continue

    if clave_actual is None:
        clave_actual = clave
        contador = valor
    elif clave == clave_actual:
        contador += valor
    else:
        print(f"{clave_actual}\t{contador}")
        clave_actual = clave
        contador = valor

if clave_actual is not None:
    print(f"{clave_actual}\t{contador}")

```

    Overwriting reducer3.py



```python
%%writefile mapper4.py

# Ejercicio 4 Mapper

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    columnas = linea.split(',')

    if columnas[7] == "AvgTemperature":
        continue

    region, country, state, city, month, day, year, temp = columnas

    try:
        temp = float(temp)
    except ValueError:
        continue

    temp_c = (temp - 32) / 1.8

    print(f"{region}\t{temp_c}")
```

    Overwriting mapper4.py



```python
%%writefile reducer4.py

# Ejercicio 4 Reducer

#!/usr/bin/env python3

import sys

region_actual = None
temp_min = None
temp_max = None

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue

    partes = linea.split('\t')
    if len(partes) != 2:
        continue

    region, temp = partes
    try:
        temp = float(temp)
    except ValueError:
        continue

    if region_actual is None:
        region_actual = region
        temp_min = temp
        temp_max = temp
    elif region == region_actual:
        if temp < temp_min:
            temp_min = temp
        if temp > temp_max:
            temp_max = temp
    else:
        print(f"{region_actual}\t{temp_min:.2f}\t{temp_max:.2f}")
        region_actual = region
        temp_min = temp
        temp_max = temp

if region_actual is not None:
    print(f"{region_actual}\t{temp_min:.2f}\t{temp_max:.2f}")

```

    Overwriting reducer4.py

