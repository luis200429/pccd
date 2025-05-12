#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#define NUM_PROCESOS 3

struct mensaje {
    long tipo;
    char texto[100];
};

int main() {
    int cola_mensajes[NUM_PROCESOS];
    key_t claves[NUM_PROCESOS];

    // Crear claves únicas para cada cola de mensajes
    for (int i = 0; i < NUM_PROCESOS; i++) {
        claves[i] = ftok(".", i + 1);
        if (claves[i] == -1) {
            perror("Error al generar clave");
            exit(EXIT_FAILURE);
        }
    }

    // Crear colas de mensajes
    for (int i = 0; i < NUM_PROCESOS; i++) {
        cola_mensajes[i] = msgget(claves[i], IPC_CREAT | 0666);
        if (cola_mensajes[i] == -1) {
            perror("Error al crear cola de mensajes");
            exit(EXIT_FAILURE);
        }
        printf("Cola de mensajes %d inicializada con ID %d\n", i, cola_mensajes[i]);
    }

    // Aquí puedes agregar lógica adicional para manejar las colas de mensajes

    return 0;
}