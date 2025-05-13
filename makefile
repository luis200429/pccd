CC = gcc
CFLAGS = -Wall -pthread

all: nodo proceso

nodo: nodo.c
	$(CC) $(CFLAGS) -o nodo2 nodo.c

proceso: proceso.c
	$(CC) $(CFLAGS) -o proceso proceso.c

clean:
	rm -f nodo proceso
