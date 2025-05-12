import matplotlib.pyplot as plt

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
                    'tipo': tipo,
                    'solicitud': solicitud,
                    'entrada': entrada,
                    'salida': salida
                })
            except ValueError:
                continue

# Ordenar por solicitud
procesos.sort(key=lambda p: p['solicitud'])

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

    # Tiempo en SC
    if p['tipo'] == 'L':
        label = 'En SC (Lector)' if 'En SC (Lector)' not in etiquetas_usadas else None
        if label: etiquetas_usadas.add(label)
        color_sc = 'blue'
    else:
        label = 'En SC (Escritor)' if 'En SC (Escritor)' not in etiquetas_usadas else None
        if label: etiquetas_usadas.add(label)
        color_sc = 'red'

    ax.barh(y, p['salida'] - p['entrada'], left=p['entrada'], height=0.4,
             color=color_sc, label=label)

# Estética
ax.set_yticks(yticks)
ax.set_yticklabels(ylabels)
ax.set_xlabel("Tiempo")
ax.set_title("Línea de tiempo por proceso: Espera, SC vacía y en SC")
handles, labels = ax.get_legend_handles_labels()
by_label = dict(zip(labels, handles))
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
