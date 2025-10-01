diccionario = {
    "manzana": "1€",
    "naranja": "3€",
    "platano": "4€",
    "pera": "2€",
    "kiwi": "5€" 
}

input = input("Introduce el nombre de una fruta: ").lower()
if input in diccionario:
    print(diccionario[input])
else:
    print("No ésta disponible")