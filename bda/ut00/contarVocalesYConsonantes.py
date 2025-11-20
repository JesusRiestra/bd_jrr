def contarVocalesYConsonantes(cadena):
    vocales = 0
    consonantes = 0
    palabra = list(cadena.lower().replace(" ",""))

    for char in palabra:
        if (char == 'a' or char == 'e' or char == 'i' or char == 'o' or char == 'u'):
            vocales+=1
        else:
            consonantes+=1

    print(f"""\
        Palabra: {cadena}
        Vocales: {vocales} 
        Consonantes: {consonantes}
          """)


contarVocalesYConsonantes("Hola Mundo")