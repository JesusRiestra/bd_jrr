cadena = input("Introduce una cadena: ")
cadenaNueva = ""
for c in cadena:
    if c.isupper():
        cadenaNueva += c.lower()
    elif c.islower():
        cadenaNueva += c.upper()
    else:
        cadenaNueva += c

print(cadenaNueva)