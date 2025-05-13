#!/bin/bash

# Verificamos si el número de argumentos es correcto
if [ "$#" -ne 7 ]; then
    echo "Error: se requieren 8 parámetros"
    echo "Uso: $0 <NUM_TOTAL_NODOS> <NUM_CONSULTAS> <NUM_RESERVAS> <NUM_ANULACIONES> <TIEMPO_SC_CONSULTAS> <TIEMPO_SC_RESERVAS> <TIEMPO_SC_ANULACIONES>"
    exit 1
fi

# Asignamos los parámetros a variables
NUM_TOTAL_NODOS=$1
NUM_CONSULTAS=$2
NUM_RESERVAS=$3
NUM_ANULACIONES=$4
TIEMPO_SC_CONSULTAS=$5
TIEMPO_SC_RESERVAS=$6
TIEMPO_SC_ANULACIONES=$7

pkill -f './nodoLE'
ipcrm --all=msg
# Lanzamos los programas con ID de nodo incremental
for (( ID_NODO=1; ID_NODO<=NUM_TOTAL_NODOS; ID_NODO++ ))
do
    echo "Lanzando nodo $ID_NODO..."
    ./nodoLE $NUM_TOTAL_NODOS $ID_NODO $NUM_CONSULTAS $NUM_RESERVAS $NUM_ANULACIONES $TIEMPO_SC_CONSULTAS $TIEMPO_SC_RESERVAS $TIEMPO_SC_ANULACIONES &
done
