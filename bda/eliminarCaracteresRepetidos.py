def eliminarCaracteresRepetidos(cadena):
    listaSinRepetidos = []
    
    for c in cadena:
        if c not in listaSinRepetidos:
            listaSinRepetidos.append(c)

    cadenaSinRepetidos = "".join(listaSinRepetidos)
    print(cadenaSinRepetidos)


eliminarCaracteresRepetidos("hola mundo que tal")