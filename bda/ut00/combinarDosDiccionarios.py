diccionario1 = {
    "naranjas": 1.20,
    "manzanas": 2.50,
    "plátanos": 3.10,
}

diccionario2 = {
    "melón": 4.20,
    "naranjas": 1.90,
}

out = diccionario1
for k, v in diccionario2.items():
    if k in out:
        out[k] += v
    else:
        out[k] = v

print(out)