asignaturas = {
    "Matemáticas": ["Ana, Carlos, Luis, María, Jorge"],
    "Física": ["Elena", "Luis", "Juan", "Sofía"],
    "Programación": ["Ana", "Carlos", "Sofía", "Jorge", "Pedro"],
    "Historia": ["María", "Juan", "Elena", "Ana"],
    "Inglés": ["Carlos", "Sofía", "Jorge", "María"]
}

salir = False
while not salir:
    print("1.- Listar estudiantes de una asignatura")
    print("2.- Matricular estudiantes")
    print("3.- Dar de baja estudiante")
    print("4.- Salir")

    usuario = input("Elige una opción: ")

    match usuario:
        case "1":
            # Listar estudiantes de una asignatura
            asignatura = input("Introduce el nombre de la asignatura: ")
            print(f"Estudiantes de {asignatura}: {asignaturas.get(asignatura)}")

        case "2":
            # Matricular alumno en una asignatura
            alumno = input("Introduce el nombre del alumno a matricular: ")
            asignatura = input("Introduce el nombre de la asignatura: ")
            if asignatura in asignaturas:
                asignaturas.get(asignatura).append(alumno)

        case "3":
            # Dar de baja a estudiante de una asignatura
            alumno = input("Introduce el nombre del alumno a dar de baja: ")
            asignatura = input("Introduce el nombre de la asignatura: ")
            if alumno in asignaturas.get(asignatura):
                asignaturas.get(asignatura).remove(alumno)
            else:
                print("El alumno no está matriculado en la asignatura!")

        case "4":
            # Salir
            salir = True
            
        case _:
            print("Opción no válida!\n")