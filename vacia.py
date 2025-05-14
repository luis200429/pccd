import matplotlib.pyplot as plt
from matplotlib.patches import Patch

# Leer y procesar datos
procesos = []
with open("datos.txt", "r") as f:
    for linea in f:
        partes = linea.strip().split()
        if len(partes) == 6:
            try:
                id_nodo = int(partes[0])
                id_proc = int(partes[1])
                solicitud = float(partes[2])
                entrada = float(partes[3])
                salida = float(partes[4])
                tipo = partes[5].strip().upper()
                if id_nodo == 1:  # Filtrar solo nodo 0
                    procesos.append({
                        'id': f'{tipo} {id_proc}',
                        'solicitud': solicitud,
                        'salida': salida,
                        'tipo': tipo
                    })
            except ValueError:
                continue

# Prioridades (mayor número = más prioridad)
prioridad_tipo = {'N': 5, 'A': 4, 'P': 3, 'R': 2, 'C': 1}
colores = {
    'C': '#377eb8',  # Azul
    'R': '#ff7f00',  # Naranja
    'N': '#984ea3',  # Púrpura
    'P': '#4daf4a',  # Verde
    'A': '#e41a1c'   # Rojo
}

# Recolectar todos los puntos de inicio y fin
puntos = set()
for p in procesos:
    puntos.add(p['solicitud'])
    puntos.add(p['salida'])
puntos = sorted(puntos)

# Crear intervalos disjuntos
intervalos = []
for i in range(len(puntos) - 1):
    ini, fin = puntos[i], puntos[i + 1]
    activos = [p for p in procesos if p['solicitud'] < fin and p['salida'] > ini]
    if activos:
        # Seleccionar el de mayor prioridad
        top = max(activos, key=lambda p: prioridad_tipo.get(p['tipo'], 0))
        intervalos.append({
            'inicio': ini,
            'fin': fin,
            'tipo': top['tipo'],
            'id': top['id']
        })

# Dibujar
plt.figure(figsize=(12, 2))
for seg in intervalos:
    dur = seg['fin'] - seg['inicio']
    plt.barh(0, dur, left=seg['inicio'], height=0.5,
             color=colores.get(seg['tipo'], 'gray'), edgecolor='black')
    plt.text(seg['inicio'] + dur / 2, 0, seg['id'], ha='center', va='center', fontsize=8, color='white')

# Leyenda manual
leyenda_patches = [
    Patch(color=colores['N'], label='Anulación (N)'),
    Patch(color=colores['A'], label='Administración (A)'),
    Patch(color=colores['P'], label='Pago (P)'),
    Patch(color=colores['R'], label='Reserva (R)'),
    Patch(color=colores['C'], label='Consulta (C)')
]

# Estética
plt.yticks([])
plt.xlabel('Tiempo')
plt.title('Línea de tiempo de la SC - Nodo 0 (prioridad visible)')
plt.legend(handles=leyenda_patches, bbox_to_anchor=(1.01, 1), loc='upper left')
plt.grid(True, axis='x', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
