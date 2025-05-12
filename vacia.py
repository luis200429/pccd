import matplotlib.pyplot as plt

# Leer y procesar datos
procesos = []
with open("datos.txt", "r") as f:
    for linea in f:
        partes = linea.strip().split()
        if len(partes) == 6:
            try:
                id_nodo = int(partes[0])
                id_proc = int(partes[1])
                solicitud = float(partes[3])
                entrada = float(partes[2])
                salida = float(partes[4])
                tipo = partes[5].upper()
                procesos.append({
                    'id': f'{id_nodo}-{id_proc}',
                    'entrada': entrada,
                    'salida': salida,
                    'tipo': tipo
                })
            except ValueError:
                continue

# Ordenar por entrada a SC
procesos.sort(key=lambda p: p['entrada'])

# Colores por tipo
colores = {'L': '#1f77b4', 'E': '#d62728'}  # azul, rojo

# Crear figura
plt.figure(figsize=(12, 2))
for p in procesos:
    duracion = p['salida'] - p['entrada']
    plt.barh(0, duracion, left=p['entrada'], height=0.5, color=colores.get(p['tipo'], 'gray'), edgecolor='black')
    plt.text(p['entrada'] + duracion / 2, 0, p['id'], ha='center', va='center', fontsize=8, color='white')

# Estética
plt.yticks([])
plt.xlabel('Tiempo')
plt.title('Línea de tiempo de la Sección Crítica (SC)')
plt.grid(True, axis='x', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
