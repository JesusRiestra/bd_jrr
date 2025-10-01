cadena = input("Introduce una cadena: ")
cadenaInvertida = cadena[::-1]

if (cadena.lower().replace(" ","") == cadenaInvertida.lower().replace(" ","")):
    print(f"""\
        Cadena: {cadena}
        Es un palíndromo""")
else:
    print(f"""\
        Cadena: {cadena}
        No es un palíndromo""")
