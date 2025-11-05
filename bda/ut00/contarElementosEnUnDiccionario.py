productos = {
"Electrónica": ["Smartphone", "Laptop", "Tablet", "Auriculares", "Smartwatch"],
"Hogar": ["Aspiradora", "Microondas", "Lámpara", "Sofá", "Cafetera"],
"Ropa": ["Camisa", "Pantalones", "Chaqueta", "Zapatos", "Bufanda"],
"Deportes": ["Pelota de fútbol", "Raqueta de tenis", "Bicicleta", "Pesas", "Cuerda de saltar"],
"Juguetes": ["Muñeca", "Bloques de construcción", "Peluche", "Rompecabezas", "Coche de juguete"]
}

categorias = 0
productos_por_categoria = 0
productos_totales = 0

for categoria in productos:
    categorias += 1

for categoria in productos:
    for producto in productos.get(categoria):
        productos_totales += 1

print(f"Hay {categorias} categorías")    
print(f"Hay {productos_totales} productos")