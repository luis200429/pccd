#!/bin/bash

# Verificamos si el número de argumentos es correcto
if [ "$#" -ne 5 ]; then
    echo "Error: se requieren 8 parámetros"
    echo "Uso: $0 <NUM_TOTAL_NODOS> <NUM_CONSULTAS> <NUM_RESERVAS>  <TIEMPO_SC_CONSULTAS> <TIEMPO_SC_RESERVAS>"
    exit 1
fi

# Asignamos los parámetros a variables
NUM_TOTAL_NODOS=$1
NUM_CONSULTAS=$2
NUM_RESERVAS=$3
TIEMPO_SC_CONSULTAS=$4
TIEMPO_SC_RESERVAS=$5

pkill -f './nodo2'
ipcrm --all=msg
# Lanzamos los programas con ID de nodo incremental
for (( ID_NODO=1; ID_NODO<=NUM_TOTAL_NODOS; ID_NODO++ ))
do
    echo "Lanzando nodo $ID_NODO..."
    ./nodo2 $NUM_TOTAL_NODOS $ID_NODO $NUM_CONSULTAS $NUM_RESERVAS $TIEMPO_SC_CONSULTAS $TIEMPO_SC_RESERVAS &
done
