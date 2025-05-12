import matplotlib.pyplot as plt

# Leer y procesar datos
datos = []
with open("datos.txt", "r") as f:
    for linea in f:
        partes = linea.strip().split()
        if len(partes) >= 6:
            try:
                ts_solicitud = float(partes[2])
                ts_entrada = float(partes[3])
                tipo = partes[5].upper()
                tiempo_espera = ts_entrada - ts_solicitud
                datos.append((ts_solicitud, tiempo_espera, tipo))
            except ValueError:
                continue  # Ignora líneas con errores de conversión numérica

# Separar por tipo
x_L = [x for x, y, t in datos if t == "L"]
y_L = [y for x, y, t in datos if t == "L"]
x_E = [x for x, y, t in datos if t == "E"]
y_E = [y for x, y, t in datos if t == "E"]

# Verificación
print(f"Procesos L: {len(x_L)}, Procesos E: {len(x_E)}, Total: {len(datos)}")

# Graficar
plt.figure(figsize=(10, 6))
plt.scatter(x_L, y_L, marker='o', color='blue', label='Lector (L)')
plt.scatter(x_E, y_E, marker='x', color='red', label='Escritor (E)')

plt.xlabel("Instante de llegada (timestamp solicitud SC)")
plt.ylabel("Tiempo de espera (entrada - solicitud)")
plt.title("Tiempo de espera por tipo de proceso")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
