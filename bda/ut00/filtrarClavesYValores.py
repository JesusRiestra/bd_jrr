diccionario = {
    "Ana": 1100,
    "Luis": 1300,
    "Elena": 1600,
    "Carlos": 1200,
    "Pedro": 1500,
    "Laura": 1300
}

salario_minimo = int(input("Introduce el salario MÍNIMO: "))
salario_maximo = int(input("Introduce el salario MÁXIMO: "))

for elem in diccionario:
    if diccionario.get(elem) >= salario_minimo and diccionario.get(elem) <= salario_maximo:
        print(f"{elem} - {diccionario.get(elem)}€")