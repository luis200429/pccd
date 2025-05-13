CC = gcc
CFLAGS = -Wall -pthread

all: nodo 

nodo: nodoLE.c
	$(CC) $(CFLAGS) -o nodo nodoLE.c

clean:
	rm -f nodoLE proceso
