frase = "Especializacion en Inteligencia Artificial y Big Data"
resultado = {}

for char in frase.lower():
    if char in resultado:
        resultado[char] += 1
    else:
        resultado[char] = 1

print(resultado)