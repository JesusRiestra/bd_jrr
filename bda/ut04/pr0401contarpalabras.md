```python
%%writefile mapper_pr0401_1.py

# Ejercicio 1 y 2 Mapper

#!/usr/bin/env python3

import sys

palabras_no_significativas = ["de", "la", "el", "y", "en", "ue", "a", "os", "el", "se"]
for linea in sys.stdin:
    linea = linea.strip()
    palabras = linea.split(' ')

    for palabra in palabras:
        palabra = palabra.lower()
        
        palabra_limpia = ""
        
        for letra in palabra:
            if letra.isalpha():
                palabra_limpia += letra
            
        if (palabra_limpia != "") and (palabra_limpia not in palabras_no_significativas):
            print(f"{palabra_limpia}\t1")
```

    Overwriting mapper_pr0401_1.py



```python
%%writefile reducer_pr0401_1.py

# Ejercicio 1 Reducer

#!/usr/bin/env python3

import sys

palabra_actual = None
contador = 0

for linea in sys.stdin:
    linea = linea.strip()

    palabra, valor = linea.split('\t')

    try:
        valor = int(valor)
    except ValueError:
        continue
        
    if not palabra_actual:
        palabra_actual = palabra
        contador = valor

    elif palabra == palabra_actual:
        contador += valor

    else:
        print(f"{palabra_actual}\t{contador}")
        palabra_actual = palabra
        contador = valor

if palabra_actual is not None:
    print(f"{palabra_actual}\t{contador}")
```

    Overwriting reducer_pr0401_1.py



```python
%%writefile mapper_pr0401_3.py

# Ejercicio 3 Mapper

#!usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    palabra, conteo = linea.split()

    print(f"{conteo}\t{palabra}")
```

    Overwriting mapper_pr0401_3.py



```python
%%writefile reducer_pr0401_3.py

# Ejercicio 3 Reducer

#!usr/bin/env python3

import sys

for linea in sys.stdin:
    linea = linea.strip()
    conteo, palabra = linea.split("\t")

    print(f"{palabra}\t{conteo}")
```

    Overwriting reducer_pr0401_3.py

