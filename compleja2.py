import matplotlib.pyplot as plt
from matplotlib.patches import Patch

# Leer y procesar datos
procesos = []
with open("datos.txt", "r") as f:
    for linea in f:
        partes = linea.strip().split()
        if len(partes) == 6:
            try:
                nodo = int(partes[0])
                proc = int(partes[1])
                solicitud = float(partes[2])
                entrada = float(partes[3])
                salida = float(partes[4])
                tipo = partes[5].upper()
                procesos.append({
                    'id': f'{tipo} {nodo}-{proc}',
                    'nodo': nodo,
                    'tipo': tipo,
                    'solicitud': solicitud,
                    'entrada': entrada,
                    'salida': salida
                })
            except ValueError:
                continue

# Ordenar por nodo y luego por solicitud
procesos.sort(key=lambda p: (p['nodo'], p['solicitud']))

# Intervalos de SC
intervalos_SC = [(p['entrada'], p['salida']) for p in procesos]

def sc_vacia_durante(ini, fin, intervalos):
    vacio = []
    actual = ini
    for e, s in sorted(intervalos):
        if s <= ini or e >= fin:
            continue
        if e > actual:
            vacio.append((actual, e))
        actual = max(actual, s)
    if actual < fin:
        vacio.append((actual, fin))
    return vacio

# Paleta de colores por tipo
colores_tipo = {
    'C': '#377eb8',  # Azul
    'R': '#ff7f00',  # Naranja
    'N': '#984ea3',  # Púrpura
    'P': '#4daf4a',  # Verde
    'A': '#e41a1c'   # Rojo
}

# Etiquetas para leyenda
etiquetas_tipo = {
    'C': 'En SC (Consulta)',
    'R': 'En SC (Reserva)',
    'N': 'En SC (Anulación)',
    'P': 'En SC (Pago)',
    'A': 'En SC (Administración)'
}

colores_forzados = {
    'En SC (Consulta)': '#377eb8',
    'En SC (Reserva)': '#ff7f00',
    'En SC (Anulación)': '#984ea3',
    'En SC (Pago)': '#4daf4a',
    'En SC (Administración)': '#e41a1c',
}

# Crear figura
fig, ax = plt.subplots(figsize=(14, len(procesos) * 0.3))
yticks = []
ylabels = []

etiquetas_usadas = set()
tiempo_espera_total = 0
tiempo_sc_vacia_con_espera = 0

for i, p in enumerate(procesos):
    y = i
    yticks.append(y)
    ylabels.append(p['id'])

    espera = p['entrada'] - p['solicitud']
    tiempo_espera_total += espera

    # Espera total
    label = 'Espera total' if 'Espera total' not in etiquetas_usadas else None
    if label: etiquetas_usadas.add(label)
    ax.barh(y, espera, left=p['solicitud'], height=0.4,
            color='gold', label=label)

    # SC vacía durante espera
    vacios = sc_vacia_durante(p['solicitud'], p['entrada'], intervalos_SC)
    label = 'SC vacía durante espera' if 'SC vacía durante espera' not in etiquetas_usadas else None
    if vacios and label: etiquetas_usadas.add(label)
    for v_ini, v_fin in vacios:
        ax.barh(y, v_fin - v_ini, left=v_ini, height=0.4,
                color='orange', label=label)
        tiempo_sc_vacia_con_espera += (v_fin - v_ini)
        label = None

    # Tiempo en SC según tipo
    tipo = p['tipo']
    color_sc = colores_tipo.get(tipo, 'gray')
    label = etiquetas_tipo.get(tipo)
    if label and label not in etiquetas_usadas:
        etiquetas_usadas.add(label)
    else:
        label = None

    ax.barh(y, p['salida'] - p['entrada'], left=p['entrada'], height=0.4,
            color=color_sc, label=label)

# Estética
ax.set_yticks(yticks)
ax.set_yticklabels(ylabels)
ax.set_xlabel("Tiempo")
ax.set_title("Línea de tiempo por proceso: Espera, SC vacía y en SC (ordenados por nodo)")
handles, labels = ax.get_legend_handles_labels()
by_label = dict(zip(labels, handles))
for label, color in colores_forzados.items():
    if label not in by_label:
        by_label[label] = Patch(color=color, label=label)
ax.legend(by_label.values(), by_label.keys())
ax.grid(True, axis='x', linestyle='--', alpha=0.5)

# Calcular y mostrar porcentaje
if tiempo_espera_total > 0:
    porcentaje_vacio = (tiempo_sc_vacia_con_espera / tiempo_espera_total) * 100
    texto_resumen = (f"SC vacía durante espera: {tiempo_sc_vacia_con_espera:.6f} s\n"
                     f"Tiempo total de espera: {tiempo_espera_total:.6f} s\n"
                     f"Porcentaje: {porcentaje_vacio:.2f}%")
else:
    texto_resumen = "No hubo espera registrada."

# Mostrar texto en la figura
ax.text(1.01, 1.01, texto_resumen, transform=ax.transAxes,
        verticalalignment='top', horizontalalignment='left',
        fontsize=10, bbox=dict(facecolor='white', alpha=0.7, edgecolor='gray'))

plt.tight_layout()
plt.show()
