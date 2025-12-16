```python
!head -n 10000 logfiles.log > server_logs_small.log
```


```python
%%writefile mapper1_pr0403.py

# Ejercicio 1

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split()
    codigo_estado = columnas[8]

    if codigo_estado.isdigit():
        print(f"{codigo_estado}\t1")
```

    Overwriting mapper1_pr0403.py



```python
%%writefile reducer1_pr0403.py

# Ejercicio 1

#!/usr/bin/env python3

import sys

codigo_actual = None
contador = 0

for linea in sys.stdin:
    codigo, valor = linea.strip().split("\t")
    valor = int(valor)

    if codigo == codigo_actual:
        contador += valor
    else:
        if codigo_actual:
            print(f"{codigo_actual}\t{contador}")
        codigo_actual = codigo
        contador = valor

if codigo_actual:
    print(f"{codigo_actual}\t{contador}")
```

    Overwriting reducer1_pr0403.py



```python
%%writefile mapper2_pr0403.py

# Ejercicio 2

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split()
    ip = columnas[0]
    respuesta = columnas[9]

    if respuesta == "-":
        respuesta = 0
    else:
        respuesta = int(respuesta)
        
    print(f"{ip}\t{respuesta}")
```

    Writing mapper2_pr0403.py



```python
%%writefile reducer2_pr0403.py

# Ejercicio 2

#!/usr/bin/env python3

import sys

ip_actual = None
contador = 0

for linea in sys.stdin:
    ip, respuesta = linea.strip().split("\t")
    respuesta = int(respuesta)

    if ip == ip_actual:
        respuesta += contador
    else:
        if ip_actual:
            print(f"{ip_actual}\t{contador}")
        ip_actual = ip
        contador = respuesta

if ip_actual:
    print(f"{ip_actual}\t{contador}")
```

    Writing reducer2_pr0403.py



```python
%%writefile mapper3_pr0403.py

# Ejercicio 3

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split()
    url = columnas[6]

    print(f"{url}\t1")
```

    Writing mapper3_pr0403.py



```python
%%writefile reducer3_pr0403.py

# Ejercicio 3

#!/usr/bin/env python3

import sys

url_actual = None
contador = 0

for linea in sys.stdin:
    url, valor = linea.strip().split("\t")
    valor = int(valor)

    if url == url_actual:
        contador += valor
    else:
        if url_actual:
            print(f"{url_actual}\t{contador}")
        url_actual = url
        contador = valor

if url_actual:
    print(f"{url_actual}\t{contador}")
```

    Writing reducer3_pr0403.py



```python
%%writefile mapper4_pr0403.py

# Ejercicio 4

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split()
    metodo = columnas[5].replace('""', '')

    print(f"{metodo}\t1")
```

    Writing mapper4_pr0403.py



```python
%%writefile reducer4_pr0403.py

# Ejercicio 4

#!/usr/bin/env python3

import sys

metodo_actual = None
contador = 0

for linea in sys.stdin:
    metodo, valor = linea.strip().split("\t")
    valor = int(valor)

    if metodo == metodo_actual:
        contador += valor
    else:
        if metodo_actual:
            print(f"{metodo_actual}\t{contador}")
        metodo_actual = metodo
        contador = valor

if metodo_actual:
    print(f"{metodo_actual}\t{contador}")
```

    Writing reducer4_pr0403.py



```python
%%writefile mapper5_pr0403.py

# Ejercicio 5

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split()
    navegador = columnas[11]

    print(f"{navegador}\t1")
```

    Writing mapper5_pr0403.py



```python
%%writefile reducer5_pr0403.py

# Ejercicio 5

#!/usr/bin/env python3

import sys

navegador_actual = None
contador = 0

for linea in sys.stdin:
    navegador, valor = linea.strip().split("\t")
    valor = int(valor)

    if navegador == navegador_actual:
        contador += valor
    else:
        if navegador_actual:
            print(f"{navegador_actual}\t{contador}")
        navegador_actual = navegador
        contador = valor

if navegador_actual:
    print(f"{navegador_actual}\t{contador}")
```

    Writing reducer5_pr0403.py



```python
%%writefile mapper6_pr0403.py

# Ejercicio 6

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split()

    fecha = columnas[3]
    fecha = fecha.lstrip("[")
    hora = fecha.split(":")[1]

    print(f"{hora}\t1")
```

    Writing mapper6_pr0403.py



```python
%%writefile reducer6_pr0403.py

# Ejercicio 6

#!/usr/bin/env python3

import sys

hora_actual = None
contador = 0

for linea in sys.stdin:
    hora, valor = linea.strip().split("\t")
    valor = int(valor)

    if hora == hora_actual:
        contador += valor
    else:
        if hora_actual:
            print(f"{hora_actual}\t{contador}")
        hora_actual = hora
        contador = valor

if hora_actual:
    print(f"{hora_actual}\t{contador}")
```

    Writing reducer6_pr0403.py



```python
%%writefile mapper7_pr0403.py

# Ejercicio 7

#!/usr/bin/env python3

import sys

for linea in sys.stdin:
    columnas = linea.strip().split()

    url = columnas[6]
    codigo = columnas[8]

    if codigo.isdigit():
        codigo = int(codigo)
        if codigo >= 400:
            print(f"{url}\t0,1")
        else:
            print(f"{url}\t1,0")
```

    Overwriting mapper7_pr0403.py



```python
%%writefile reducer7_pr0403.py

# Ejercicio 7

#!/usr/bin/env python3

import sys

url_actual = None
exitos = 0
errores = 0

for linea in sys.stdin:
    url, valor = linea.strip().split("\t")
    exito, error = valor.split(",")
    exito = int(exito)
    error = int(error)

    if url == url_actual:
        exitos += exito
        errores += error
    else:
        if url_actual is not None:
            total = exitos + errores
            porcentaje = (errores / total) * 100 if total > 0 else 0
            print(f"{url_actual}\t{exitos}\t{errores}\t{porcentaje:.2f}%")
        url_actual = url
        exitos = exito
        errores = error

if url_actual is not None:
    total = exitos + errores
    porcentaje = (errores / total) * 100 if total > 0 else 0
    print(f"{url_actual}\t{exitos}\t{errores}\t{porcentaje:.2f}%")
```

    Overwriting reducer7_pr0403.py

