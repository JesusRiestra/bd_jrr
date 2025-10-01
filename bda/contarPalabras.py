def contarPalabras(cadena):
    palabras = 0

    for i in cadena.split(" "):
        palabras+=1

    print(f"Número de palabras: {palabras}")


contarPalabras("Hola mundo, que tal!")