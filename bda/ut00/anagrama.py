primeraCadena = input("Introduce la primera cadena: ").lower().split("")
segundaCadena = input("Introduce la segunda cadena: ").lower().split("")

if (primeraCadena.sort() == segundaCadena.sort()):
    print(f"{primeraCadena} y {segundaCadena} son un anagrama")
else:
    print(f"{primeraCadena} y {segundaCadena} no son un anagrama")