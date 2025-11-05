n = int(input("Introduce el primer número: "))
k = int(input("Introduce el segundo número: "))

for i in range(k):
    print(f"{n} * {i + 1} = {n * (i + 1)}")