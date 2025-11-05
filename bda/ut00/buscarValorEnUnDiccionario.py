diccionario = {
    "manzana": "1€",
    "naranja": "3€",
    "platano": "4€",
    "pera": "2€",
    "kiwi": "5€" 
}

fruta = input("Introduce el nombre de una fruta: ").lower()
if fruta in diccionario:
    print(diccionario[fruta])
else:
    print("No ésta disponible")