// proceso.c
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/msg.h>

struct mensaje {
    long tipo;
    char texto[100];
    int ticket;
    int id;
    int nodo;
};

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Uso: %s <nodo (1|2|3)>\n", argv[0]);
        exit(1);
    }

    int nodo = atoi(argv[1]);
    int cola = msgget(100 + nodo, 0666);  // Cola interna

    if (cola == -1) {
        perror("msgget");
        exit(1);
    }

    struct mensaje msg;
    
    while (1) {  // Bucle infinito para simular múltiples entradas a la SC

        printf("[Proceso interno] Presiona Enter para solicitar acceso a la Sección Crítica...\n");
        getchar();  // El proceso espera a que el usuario presione Enter para continuar
        msg.tipo = 3;
        msgsnd(cola, &msg, sizeof(msg) - sizeof(long), 0);
        printf("[Proceso interno] Solicita acceso a nodo %d\n", nodo);

        // Espera permiso
        msgrcv(cola, &msg, sizeof(msg) - sizeof(long), 4, 0);
        printf("[Proceso interno] Entrando en la Sección Crítica\n");

        // Simula trabajo en la sección crítica
        printf("[Proceso interno] Presiona Enter para salir de la Sección Crítica...\n");
        getchar();  // El proceso espera a que el usuario presione Enter para continuar

        printf("[Proceso interno] Saliendo de la Sección Crítica\n");
        msg.tipo = 5;
        msgsnd(cola, &msg, sizeof(msg) - sizeof(long), 0);
        
        // El proceso vuelve a solicitar acceso y repite el ciclo
    }

    return 0;
}
