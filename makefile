CC = gcc
CFLAGS = -Wall -pthread

all: nodoLE proceso

nodo: nodoLE.c
	$(CC) $(CFLAGS) -o nodo2 nodoLE.c

proceso: proceso.c
	$(CC) $(CFLAGS) -o proceso proceso.c

clean:
	rm -f nodo proceso
