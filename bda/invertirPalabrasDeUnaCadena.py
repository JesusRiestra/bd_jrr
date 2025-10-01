cadena = input("Introduce una cadena: ")

lista = cadena.split(" ")
lista.reverse()
cadenaInvertida = " ".join(lista)
print(cadenaInvertida)