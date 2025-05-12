
// nodo.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <semaphore.h>

#define MAX_PROCESOS 10
#define MAX_NODOS 10

struct mensaje {
    long tipo;
    char texto[100];
    int ticket;
    int id;
    int nodo;
};

sem_t semaforo;
int num_pend = 0;
int mi_tiquet = 0;
int mi_id;
int mi_nodo;
int total_nodos;
int id_nodos[MAX_NODOS];
int nodos[MAX_NODOS];
int id_nodos_pend[MAX_NODOS] = {0};
int quiero = 0;
int max_tiquet = 0;
int num_replies = 0;
int cola_interna;

void* receptor_externo(void* arg) {
    struct mensaje msg;

    while(1) {
        msgrcv(mi_id, &msg, sizeof(struct mensaje) - sizeof(long), 1, 0);

        int id_nodo_origen = msg.id;
        int ticket_origen = msg.ticket;
        int nodo_origen = msg.nodo;

        printf("[Nodo %d] Recibido REQUEST de nodo %d con tiquet %d\n", mi_nodo, nodo_origen, ticket_origen);

        sem_wait(&semaforo);

        if (ticket_origen > mi_tiquet)
            max_tiquet = ticket_origen;

        if ((!quiero)||(ticket_origen<mi_tiquet)||(ticket_origen==mi_tiquet&&(id_nodo_origen<mi_id))) {
            msg.tipo = 2;
            msg.id = mi_id;
            msg.nodo = mi_nodo;
            msgsnd(id_nodo_origen, &msg, sizeof(struct mensaje) - sizeof(long), 0);
            printf("[Nodo %d] Enviado REPLY a nodo %d\n", mi_nodo, nodo_origen);
        } else {
            id_nodos_pend[num_pend++] = id_nodo_origen;
        }

        sem_post(&semaforo);
    }
    return NULL;
}
int procesos_en_sc = 0;      // Cuántos procesos internos están en la SC
int procesos_esperando = 0;  // Cuántos están esperando entrar

void* receptor_interno(void* arg) {
    struct mensaje msg;

    while (1) {
        // Espera solicitud de entrada
        msgrcv(cola_interna, &msg, sizeof(struct mensaje) - sizeof(long), 3, 0);

        sem_wait(&semaforo);
        procesos_esperando++;

        if (procesos_en_sc == 0) {
            // Primera solicitud interna: iniciar protocolo Ricart-Agrawala
            quiero = 1;
            mi_tiquet = max_tiquet + 1;
            sem_post(&semaforo);

            printf("[Nodo %d] Proceso interno inicia protocolo con ticket %d\n", mi_nodo, mi_tiquet);

            msg.tipo = 1;
            msg.ticket = mi_tiquet;
            msg.id = mi_id;
            msg.nodo = mi_nodo;

            for (int i = 0; i < total_nodos - 1; i++) {
                msgsnd(id_nodos[i], &msg, sizeof(struct mensaje) - sizeof(long), 0);
                printf("[Nodo %d] Enviado REQUEST a nodo %d\n", mi_nodo, nodos[i]);
            }

            // Espera replies
            num_replies = 0;
            struct mensaje rmsg;
            while (num_replies < total_nodos - 1) {
                msgrcv(mi_id, &rmsg, sizeof(struct mensaje) - sizeof(long), 2, 0);
                num_replies++;
                printf("[Nodo %d] Recibido REPLY de nodo %d\n", mi_nodo, rmsg.nodo);
            }

            sem_wait(&semaforo);
            procesos_en_sc++;
            procesos_esperando--;
            sem_post(&semaforo);

            // Notifica al proceso interno que puede entrar
            msg.tipo = 4;
            msgsnd(cola_interna, &msg, sizeof(struct mensaje) - sizeof(long), 0);
            printf("[Nodo %d] Permiso enviado a proceso interno (primero)\n", mi_nodo);
        } else {
            // Hay otro proceso en SC, este solo espera turno
            sem_post(&semaforo);

            // Espera a que lo despierten (turno local)
            msg.tipo = 4;
            msgsnd(cola_interna, &msg, sizeof(struct mensaje) - sizeof(long), 0);
            printf("[Nodo %d] Permiso local a proceso interno (en cola)\n", mi_nodo);
        }

        // Espera que el proceso interno termine
        msgrcv(cola_interna, &msg, sizeof(struct mensaje) - sizeof(long), 5, 0);
        printf("[Nodo %d] Proceso interno ha salido de la SC\n", mi_nodo);

        sem_wait(&semaforo);
        procesos_en_sc--;

        if (procesos_en_sc == 0 && procesos_esperando == 0) {
            // Todos los procesos internos terminaron: liberar SC global
            quiero = 0;
            msg.tipo = 2;
            msg.ticket = 0;
            msg.nodo = mi_nodo;

            for (int k = 0; k < num_pend; k++) {
                msgsnd(id_nodos_pend[k], &msg, sizeof(struct mensaje) - sizeof(long), 0);
                printf("[Nodo %d] Enviado REPLY pendiente a nodo %d\n", mi_nodo, id_nodos_pend[k]);
            }

            num_pend = 0;
        }

        sem_post(&semaforo);
    }
    return NULL;
}
int main(int argc, char *argv[]) {
    if(argc != 3) {
        fprintf(stderr, "Uso: %s <NUM_TOTAL_NODOS> <ID_NODO_ACTUAL>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    pthread_t hilo_ext, hilo_int;

    total_nodos = atoi(argv[1]);
    mi_nodo = atoi(argv[2]);

    if (total_nodos < 2 || total_nodos > MAX_NODOS || mi_nodo < 1 || mi_nodo > total_nodos) {
        fprintf(stderr, "Parámetros inválidos.\n");
        exit(EXIT_FAILURE);
    }

    int idx = 0;
    for (int i = 1; i <= total_nodos; i++) {
        if (i != mi_nodo) {
            nodos[idx] = i;
            id_nodos[idx] = msgget(i, IPC_CREAT | 0666);
            idx++;
        }
    }

    mi_id = msgget(mi_nodo, IPC_CREAT | 0666);
    cola_interna = msgget(100 + mi_nodo, IPC_CREAT | 0666);

    sem_init(&semaforo, 0, 1);

    pthread_create(&hilo_ext, NULL, receptor_externo, NULL);
    pthread_create(&hilo_int, NULL, receptor_interno, NULL);

    pthread_join(hilo_ext, NULL);
    pthread_join(hilo_int, NULL);

    sem_destroy(&semaforo);
    return 0;
}