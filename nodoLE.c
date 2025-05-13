#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <semaphore.h>
#include <time.h>
#include <sys/time.h>


#define consulta 0
#define reserva 1
#define anulacion 4

struct mensaje {
    long tipo;
    int ticket;
    int id;
    int nodo;
    int tipo_proceso;
};


float tiempo_sc_consultas = 0;
float tiempo_sc_reservas = 0;
float tiempo_sc_anulaciones = 0;
float tiempo_sc_administracion = 0;
float tiempo_sc_pagos = 0;



int num_pend = 0;
int mi_tiquet = 0;
int mi_id;
int mi_nodo;
int num_nodos;
int* nodos;
int* id_nodos;
int* id_nodos_pend;
int* tipo_nodos_pend;
int* tickets_pendientes;
int quiero = 0;
int dentro = 0;
int* dentro_array;
int tipo_actual = -1;
int tipo_pendiente = -1;
int max_tiquet = 0;
int en_sc = 0;
int cola_reservas = 0;
int cola_consultas = 0;
int cola_anulaciones = 0;
int ticket_esperado = 0;//PROTEGER?


int respuestas_recibidas = 0;

int maxProcesos = 4;

sem_t sem_quiero, sem_tipo_actual, sem_tiquet, sem_max_tiquet, sem_sc_reservas,sem_sc_anulaciones;
sem_t sem_cola_reservas, sem_cola_consultas, sem_pend, sem_cola_anulaciones;
sem_t sem_max_procesos, sem_sc_consultas, sem_respuestas_recibidas;
sem_t sem_bloqueo_consultas, sem_dentro,sem_tipo_pendiente, sem_fichero;
sem_t* sems_sc;





/////////////////////////////////////////////////////////FUNCIONES//////////////////////////////////////////////////////////////
void solicitar_seccion_critica(int tipo_proceso) {//indica quien solicita

    sem_wait(&sem_tipo_actual); tipo_actual = tipo_proceso; sem_post(&sem_tipo_actual);

    sem_wait(&sem_respuestas_recibidas); respuestas_recibidas = 0; sem_post(&sem_respuestas_recibidas);

    sem_wait(&sem_quiero); quiero = 1; sem_post(&sem_quiero);   

    struct mensaje msg;
    msg.tipo = 1;
    sem_wait(&sem_max_tiquet);
    msg.ticket = max_tiquet + 1;//////////////////////////////////////////////////////////////////////
    max_tiquet = msg.ticket;
    sem_post(&sem_max_tiquet);
    sem_wait(&sem_tiquet);
    ticket_esperado = msg.ticket;
    sem_post(&sem_tiquet);
    msg.id = mi_id;
    msg.nodo = mi_nodo;
    msg.tipo_proceso = tipo_proceso;

    for (int i = 0; i < num_nodos - 1; i++) {
        msgsnd(id_nodos[i], &msg, sizeof(struct mensaje) - sizeof(long), 0);
        printf("Enviado REQUEST a nodo %d con ticket %d\n", nodos[i], msg.ticket);
    }

    
}

void liberar_seccion_critica(int TIPO_PROCESO) {

    struct mensaje msg;
    msg.tipo = 2;
    msg.ticket = 0;
    msg.nodo = mi_nodo;
    msg.tipo_proceso = TIPO_PROCESO;



    sem_wait(&sem_pend);
    int nuevos_pend = 0;
    for (int k = 0; k < num_pend; k++) {
        if (tipo_nodos_pend[k] >= TIPO_PROCESO) {

            msg.ticket = tickets_pendientes[k];     
            msg.tipo_proceso = tipo_nodos_pend[k];

            msgsnd(id_nodos_pend[k], &msg, sizeof(struct mensaje) - sizeof(long), 0);
            printf("Mensaje REPLAY enviado a ID %d (tipo %d)\n", id_nodos_pend[k], tipo_nodos_pend[k]);
        } else {
            // Reinsertar en la cola si aún no debe recibir REPLY
            id_nodos_pend[nuevos_pend] = id_nodos_pend[k];
            tipo_nodos_pend[nuevos_pend] = tipo_nodos_pend[k];
            tickets_pendientes[nuevos_pend] = tickets_pendientes[k];
            nuevos_pend++;
        }
    }
    num_pend = nuevos_pend;
    sem_post(&sem_pend);
/* 
    sem_wait(&sem_quiero); quiero = 0; sem_post(&sem_quiero);
    sem_wait(&sem_tipo_actual); tipo_actual = -1; sem_post(&sem_tipo_actual); */
}

void contestar_todos_replies() {
    struct mensaje msg;
    msg.tipo = 2;
    msg.id = mi_id;
    msg.nodo = mi_nodo;

    
    sem_wait(&sem_pend);
    for (int i = 0; i < num_pend; i++) {

        if(id_nodos_pend[i] == 0) continue; // Evitar enviar a nodos no válidos
        msg.ticket = tickets_pendientes[i];     
        msg.tipo_proceso = tipo_nodos_pend[i];
        msgsnd(id_nodos_pend[i], &msg, sizeof(struct mensaje) - sizeof(long), 0);
        printf("[Nodo %d] Enviado REPLY a nodo %d (tipo %d)\n", mi_nodo, id_nodos_pend[i], tipo_nodos_pend[i]);
    }
    num_pend = 0; // Limpiar la cola de pendientes
    sem_post(&sem_pend);
    sem_wait(&sem_quiero); quiero = 0; sem_post(&sem_quiero);

}






int hay_reservas_pendientes() {
    int resultado = 0;
    sem_wait(&sem_pend);
    for (int i = 0; i < num_pend; i++) {
        if (tipo_nodos_pend[i] == reserva) {
            resultado = 1;
            break;
        }
    }
    sem_post(&sem_pend);
    return resultado;
}

int hay_anulaciones_pendientes() {
    int resultado = 0;
    sem_wait(&sem_pend);
    for (int i = 0; i < num_pend; i++) {
        if (tipo_nodos_pend[i] == anulacion) {
            resultado = 1;
            break;
        }
    }
    sem_post(&sem_pend);
    return resultado;
}

///////////////////////////////////////////////////////RECEPTOR//////////////////////////////////////////////////////

void* receptor(void* arg) {

    //usleep((rand() % 8000000)); // Sleep for a random time between 0 and 200 milliseconds
    struct mensaje msg;
    int solicita = 0;

    while(1) {
        msgrcv(mi_id, &msg, sizeof(struct mensaje) - sizeof(long), -2, 0);  // Escucha ambos tipos
        // Simular retardos en la red
        //usleep((rand() % 10) * 10); // Retardo aleatorio entre 0 y 1000 ms

        if (msg.tipo == 1) {  // REQUEST

            int id_nodo_origen = msg.id;
            int ticket_origen = msg.ticket;
            int nodo_origen = msg.nodo;
            int tipo_proceso = msg.tipo_proceso;


       


            // Actualizar max_tiquet
            sem_wait(&sem_max_tiquet);

            if (ticket_origen > max_tiquet){
                max_tiquet = ticket_origen;
            }
            sem_post(&sem_max_tiquet);

            int conceder = 0;

            sem_wait(&sem_dentro);
            if(dentro == 1) {

                sem_post(&sem_dentro);

                sem_wait(&sem_cola_reservas);
                if(cola_reservas==0) {//si en la sc hay consultas dejo pasar
                    sem_post(&sem_cola_reservas);

                    if(tipo_proceso == consulta) {//consultas en sc y llega apet de consulta

                        msg.id = mi_id;
                        msg.tipo = 2;
                        msg.nodo = mi_nodo;
                        msg.ticket = ticket_origen;//////////////////////
                        msg.tipo_proceso = tipo_proceso;




                        msgsnd(id_nodo_origen, &msg, sizeof(struct mensaje) - sizeof(long), 0);
                        printf("[Nodo %d] Enviado REPLY a nodo %d\n", mi_nodo, nodo_origen);
                    } else {//consultas en sc y llega pet de escritura

                            sem_wait(&sem_pend);
                            id_nodos_pend[num_pend] = id_nodo_origen;
                            tipo_nodos_pend[num_pend] = tipo_proceso;
                            tickets_pendientes[num_pend] = ticket_origen;
                            num_pend++;
                            sem_post(&sem_pend);
                            printf("[Nodo %d] NO concedido, pq hay consultas en sc almacenado pendiente a nodo %d\n", mi_nodo, nodo_origen);
                    }
                    
                } else {//si hay reservas en sc tambien almaceno
                    sem_post(&sem_cola_reservas);
                    sem_wait(&sem_pend);
                    id_nodos_pend[num_pend] = id_nodo_origen;
                    tipo_nodos_pend[num_pend] = tipo_proceso;
                    tickets_pendientes[num_pend] = ticket_origen;
                    num_pend++;
                    sem_post(&sem_pend);
                    printf("[Nodo %d] NO concedido pq hay reservas en sc, almacenado pendiente a nodo %d\n", mi_nodo, nodo_origen);
                }

            }
            else {//aqui no hay nadie dentro
                sem_post(&sem_dentro);


                sem_wait(&sem_quiero);

                if (!quiero) {
                    conceder = 1;
                    printf("NO quiero\n");
                    sem_post(&sem_quiero);
                } else{

                    sem_post(&sem_quiero);
                    sem_wait(&sem_tipo_actual);
                    if (tipo_actual < tipo_proceso) {
                        
                         solicita = tipo_actual;

                        sem_post(&sem_tipo_actual);
                        conceder = 1;
                        printf("vovliendo a pedir\n");
                        solicitar_seccion_critica(solicita);


                    } else{
                        if (tipo_actual == tipo_proceso) {
                            
                            solicita = tipo_actual;

                            sem_post(&sem_tipo_actual);

                            sem_wait(&sem_tiquet);
                            printf("mi_tiquet: %d, ticket_origen: %d, mi_id: %d, id_nodo_origen: %d\n", ticket_esperado, ticket_origen, mi_id, id_nodo_origen);
                            if (ticket_origen < ticket_esperado ||
                                (ticket_origen == ticket_esperado && id_nodo_origen < mi_id)) {
                                conceder = 1;
                            }
                            sem_post(&sem_tiquet);
                        }
                        else {
                            sem_post(&sem_tipo_actual);
                        }
                    }
                }


                if (conceder) {

                 
                    msg.id = mi_id;
                    msg.tipo = 2;
                    msg.nodo = mi_nodo;
                    msg.ticket = ticket_origen;//////////////////////

                    msg.tipo_proceso = tipo_proceso;    




                    
                    
                    msgsnd(id_nodo_origen, &msg, sizeof(struct mensaje) - sizeof(long), 0);
                    printf("[Nodo %d] Enviado REPLY a nodo %d\n", mi_nodo, nodo_origen);
                } else {
                    sem_wait(&sem_pend);
                    id_nodos_pend[num_pend] = id_nodo_origen;
                    tipo_nodos_pend[num_pend] = tipo_proceso;
                    tickets_pendientes[num_pend] = ticket_origen;
                    num_pend++;
                    sem_post(&sem_pend);
                    printf("[Nodo %d] NO concedido pq no concedido, almacenado pendiente a nodo %d\n", mi_nodo, nodo_origen);
                }   

            }

            

        } else if (msg.tipo == 2) {

            int ticket = msg.ticket;
            int tipo_proceso = msg.tipo_proceso;

            if(ticket != ticket_esperado) { //LA PRIORIDAD DEL REPLY ES LA PRIO A LA Q CREIAN Q ESTABAN CONTESTANDO
                
                printf("tirando reply\n");
                continue;
            }
            else{
 
                sem_wait(&sem_respuestas_recibidas);
                if(respuestas_recibidas < num_nodos - 1){

                    printf("Mensaje REPLAY recibido de nodo %d\n", msg.nodo);
                    respuestas_recibidas++;
                    printf("[Nodo %d] Respuestas recibidas: %d\n", mi_nodo, respuestas_recibidas);
                }
                if(respuestas_recibidas == num_nodos - 1) {

                    if(tipo_proceso == consulta) {
                        sem_post(&sem_sc_consultas);
                    }
                    else if(tipo_proceso == reserva) {
                        sem_post(&sem_sc_reservas);
                        
                    }

                    printf("[Nodo %d] Sección crítica concedida\n", mi_nodo);
                    respuestas_recibidas = 0;
                }
                sem_post(&sem_respuestas_recibidas);
            
            }
            
        }
    }
    return NULL;
}


/////////////////////////////////////////////////////////////////reserva////////////////////////////////////////////////////////////////////////////////////

void* reserva_hilo(void* arg) {

    sleep(3); // Sleep for a random time between 0 and 200 milliseconds
    int posicion;

    int contador_print_reservas = *((int*) arg);


    struct timeval t_solicita, t_entra, t_sale;    

    //1 tiempo que QUIERE entrar
    //2 tiempo que ENTRA
    //3 tiempo que SALE


    sem_wait(&sem_cola_reservas);
    posicion=cola_reservas++;
    sem_post(&sem_cola_reservas);





    gettimeofday(&t_solicita, NULL);

    //printf(("entramos qaqui\n"));
    //sem_wait(&sem_max_tiquet); mi_tiquet = max_tiquet + 1; sem_post(&sem_max_tiquet);
   
   //habria q comprobar si eres el mas prioritario del nodo, por ahora obviamos
   
   sem_wait(&sem_dentro);
   if(dentro == 0) {
        sem_post(&sem_dentro);

        if(posicion == 0) {
            solicitar_seccion_critica(reserva);
            printf("[Nodo %d] Primer reserva, solicita sección crítica distribuida\n", mi_nodo);
            sem_wait(&sem_sc_reservas);




        } else {
            sem_wait(&sem_sc_reservas);
        }

        sem_wait(&sem_dentro);
        dentro = 1;
        sem_post(&sem_dentro);

        gettimeofday(&t_entra, NULL);

        printf("[Nodo %d] reserva (posición %d) entra en la sección crítica\n", mi_nodo, posicion);
        sleep(tiempo_sc_reservas);
        printf("[Nodo %d] reserva (posición %d) sale de la sección crítica\n", mi_nodo, posicion);
        int restantes;

        gettimeofday(&t_sale, NULL);

        sem_wait(&sem_cola_reservas);

        if(cola_reservas == 1) {//soy el ultimo
            sem_post(&sem_cola_reservas);
            int hay_reserva_pendiente = hay_reservas_pendientes(); 
            

            if(hay_reserva_pendiente){

                contestar_todos_replies();//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN

                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                printf("[Nodo %d] Último reserva, libera sección crítica distribuida\n", mi_nodo);
                
                
                

                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    solicitar_seccion_critica(consulta);
                    contestar_todos_replies();


                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies();//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                }
            }
            else{//no hay reservas
                
                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    sem_wait(&sem_tipo_actual);
                    tipo_actual = consulta;////////////////////////si no no contesta replys
                    sem_post(&sem_tipo_actual);
                    //solicitar_seccion_critica(consulta);
                    sem_post(&sem_sc_consultas);//OJO CON ESTO
                    //contestar_todos_replies(10);


                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                    printf("[Nodo %d] Último reserva, libera sección crítica distribuida\n", mi_nodo);
                }

            }

        } else {//no soy el ultimo
            sem_post(&sem_cola_reservas);
            int hay_reserva_pendiente = hay_reservas_pendientes(); 

            if(hay_reserva_pendiente){

                int por_atender = 0;
                sem_wait(&sem_max_procesos);
                maxProcesos--;
                por_atender = maxProcesos;
                sem_post(&sem_max_procesos);

                if(por_atender == 0) {//se han atendido N, recordar restablecer valor

                    liberar_seccion_critica(reserva);//EN PARAMETRO A QUIEN CREEN Q CONTESTA
                    sem_wait(&sem_max_procesos);
                    maxProcesos = 4;
                    sem_post(&sem_max_procesos);

                    //AQUI HABRÁ Q AÑADIR COMPROBACION PRIORIDAD
                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);


                    printf("[Nodo %d] reserva maximo, libera sección crítica distribuida\n", mi_nodo);
                
                    solicitar_seccion_critica(reserva);

                }
                else {//no se han atendido N, no hago nada

                    /* sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro); */
                    sem_post(&sem_sc_reservas);

                }


            }
            else {//no hay reservas pendientes y no soy el ultimo

                sem_post(&sem_sc_reservas);
            }

        }

    } else {//hay alguien dentro
        sem_post(&sem_dentro);
        sem_wait(&sem_sc_reservas);

        gettimeofday(&t_entra, NULL);

        printf("[Nodo %d] reserva (posición %d) entra en la sección crítica\n", mi_nodo, posicion);
        sleep(tiempo_sc_reservas);
        printf("[Nodo %d] reserva (posición %d) sale de la sección crítica\n", mi_nodo, posicion);

        gettimeofday(&t_sale, NULL);


        int restantes;

        sem_wait(&sem_cola_reservas);

        if(cola_reservas == 1) {//soy el ultimo

            sem_post(&sem_cola_reservas);

            int hay_reserva_pendiente = hay_reservas_pendientes(); 

            if(hay_reserva_pendiente){

                contestar_todos_replies();//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN

                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                printf("[Nodo %d] Último reserva, libera sección crítica distribuida\n", mi_nodo);
            
                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    solicitar_seccion_critica(consulta);
                    contestar_todos_replies();
                    //sem_post(&sem_sc_consultas);//OJO CON ESTO

                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VAffLGA A TODOS
                }
            }
            else{//no hay reservas
                
                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    //solicitar_seccion_critica(consulta);
                    sem_wait(&sem_tipo_actual);
                    tipo_actual = consulta;////////////////////////si no no contesta replys
                    sem_post(&sem_tipo_actual);

                    sem_post(&sem_sc_consultas);//OJO CON ESTO
                    //contestar_todos_replies(10);//p/////////////////////////////////////////////////7ESTO FAI FALTA??????


                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                }

            }

        } else {//no soy el ultimo

            sem_post(&sem_cola_reservas);

            int hay_reserva_pendiente = hay_reservas_pendientes(); 

            if(hay_reserva_pendiente){

                int por_atender = 0;
                sem_wait(&sem_max_procesos);
                maxProcesos--;
                por_atender = maxProcesos;
                sem_post(&sem_max_procesos);

                if(por_atender == 0) {//se han atendido N, recordar restablecer valor

                    liberar_seccion_critica(reserva);//EN PARAMETRO A QUIEN CREEN Q CONTESTA
                    sem_wait(&sem_max_procesos);
                    maxProcesos = 4;
                    sem_post(&sem_max_procesos);

                    //AQUI HABRÁ Q AÑADIR COMPROBACION PRIORIDAD
                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);


                    printf("[Nodo %d] reserva maximo, libera sección crítica distribuida\n", mi_nodo);
                    
                    solicitar_seccion_critica(reserva);
                    //YA SE Q QUEDAN reservas, PIDEN ELLOS
                    /* sem_wait(&sem_cola_consultas);
                    if(cola_consultas > 0) {
                        sem_post(&sem_cola_consultas);
                        solicitar_seccion_critica(consulta);
                        sem_post(&sem_sc_reservas);//OJO CON ESTO

                    } else {
                        sem_post(&sem_cola_consultas);
                        contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                    } */

                }
                else {//no se han atendido N, no hago nada

                    /* sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro); */
                    sem_post(&sem_sc_reservas);

                }


            }
            else {//no hay reservas pendientes y no soy el ultimo

                sem_post(&sem_sc_reservas);
            }

        }


    }

    sem_wait(&sem_cola_reservas);
    cola_reservas--;
    sem_post(&sem_cola_reservas);
   

    FILE *archivo = fopen("datos.txt", "a");
    if (archivo == NULL) {
        perror("Error al abrir el archivo");
        return NULL;
    }
    

    double d1 = t_solicita.tv_sec + t_solicita.tv_usec / 1e6;
    double d2 = t_entra.tv_sec + t_entra.tv_usec / 1e6;
    double d3 = t_sale.tv_sec + t_sale.tv_usec / 1e6;

    sem_wait(&sem_fichero);
    fprintf(archivo, "%d %d %.6f %.6f %.6f E\n", mi_nodo, contador_print_reservas, d1, d2, d3);
    sem_post(&sem_fichero);

    // Cerrar el archivo
    fclose(archivo);


  

return NULL;


}
   

////////////////////////////////////////////////////////////////consulta////////////////////////////////////////////////////////////////////////////////////

void* consulta_hilo(void* arg) {


    usleep(3); // Sleep for a random time between 0 and 200 milliseconds
    int posicion;

    int contador_print_consultas = *((int*) arg);

    struct timeval t_solicita, t_entra, t_sale;    

    //1 tiempo que QUIERE entrar
    //2 tiempo que ENTRA
    //3 tiempo que SALE

    sem_wait(&sem_cola_consultas);
    posicion=cola_consultas++;
    sem_post(&sem_cola_consultas);




    gettimeofday(&t_solicita, NULL);

    sem_wait(&sem_cola_reservas);
    if(cola_reservas==0){//si no hay reservas, soy la mas prioritaria

        sem_post(&sem_cola_reservas);
        
        sem_wait(&sem_dentro);

        if(dentro == 0) {//no hay nadie en SC
            sem_post(&sem_dentro);

            

            if(posicion == 0) {//soy el primero
                
                solicitar_seccion_critica(consulta);
                printf("[Nodo %d] Primer consulta, solicita sección crítica distribuida\n", mi_nodo);
                sem_wait(&sem_sc_consultas);

                sem_wait(&sem_dentro);
                dentro = 1;
                sem_post(&sem_dentro);

            } 
            else {
                printf("CREO Q NO SE ENTRA AQUI EN LA VIDA\n");
                sem_wait(&sem_sc_consultas);
                
            }

        } else {//si dentro == 1 por cojones hay consultas en SC, entro
            sem_post(&sem_dentro);

        }

        gettimeofday(&t_entra, NULL);

        sem_wait(&sem_cola_consultas);
        if(cola_consultas>0) sem_post(&sem_sc_consultas);
        sem_post(&sem_cola_consultas);

        contestar_todos_replies();

        printf("[Nodo %d] consulta (posición %d) entra en la sección crítica\n", mi_nodo, posicion);
        sleep(tiempo_sc_consultas);
        printf("[Nodo %d] consulta (posición %d) sale de la sección crítica\n", mi_nodo, posicion);


        gettimeofday(&t_sale, NULL);

        int restantes;

        sem_wait(&sem_cola_consultas);
        restantes = cola_consultas-1;
        sem_post(&sem_cola_consultas);


        if(restantes==0){//soy el ultimo de mi nodo

            sem_wait(&sem_cola_reservas);
            printf("Cola de reservas: %d", cola_reservas);
            if(cola_reservas>0){

                sem_post(&sem_cola_reservas);
                
                liberar_seccion_critica(reserva);//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN
                solicitar_seccion_critica(reserva);
                printf("SC cedida a reservas locales");
            }
            else{
                sem_post(&sem_cola_reservas);

                int hay_reserva_pendiente = hay_reservas_pendientes();

                if(hay_reserva_pendiente){

                    liberar_seccion_critica(reserva);//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN

                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);


                    printf("[Nodo %d] Último consulta, libera sección crítica distribuida\n", mi_nodo);


                }
                else{

                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                    printf("[Nodo %d] Último consulta, libera sección crítica distribuida\n", mi_nodo);
                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);
                    

                }
            }
            



        }
        else{//no soy el ultimo

            //diria q te mueres y ya

        }




    } else {//hay reservas, espero a q me den paso. Ya manda  la soli el thread de reservas
        sem_post(&sem_cola_reservas);
        sem_wait(&sem_sc_consultas);//probablemente haya q dar paso a mas consultas.
        printf("consulta intenta entrar en la SC\n"); 

        gettimeofday(&t_entra, NULL);

        sem_wait(&sem_cola_consultas);
        if(cola_consultas>0) sem_post(&sem_sc_consultas);
        sem_post(&sem_cola_consultas);

        contestar_todos_replies();

        printf("[Nodo %d] consulta (posición %d) entra en la sección crítica\n", mi_nodo, posicion);
        sleep(tiempo_sc_consultas);
        printf("[Nodo %d] consulta (posición %d) sale de la sección crítica\n", mi_nodo, posicion);

        gettimeofday(&t_sale, NULL);
        int restantes;

        sem_wait(&sem_cola_consultas);
        restantes = cola_consultas-1;
        sem_post(&sem_cola_consultas);


        if(restantes==0){//soy el ultimo

            sem_wait(&sem_cola_reservas);
            printf("Cola de reservas: %d", cola_reservas);

            if(cola_reservas>0){

                sem_post(&sem_cola_reservas);

                liberar_seccion_critica(reserva);//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN

                solicitar_seccion_critica(reserva);
                
                printf("SC cedida a reservas locales");


            }
            else{

                int hay_reserva_pendiente = hay_reservas_pendientes();

                if(hay_reserva_pendiente){

                    liberar_seccion_critica(reserva);//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN

                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);


                    printf("[Nodo %d] Último consulta, libera sección crítica distribuida\n", mi_nodo);


                }
                else{

                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                    printf("[Nodo %d] Último consulta, libera sección crítica distribuida\n", mi_nodo);
                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);
                    

                }
            }
            



        }
        else{//no soy el ultimo

            //diria q te mueres y ya

        }

    }
        

    sem_wait(&sem_cola_consultas);
    cola_consultas--;
    sem_post(&sem_cola_consultas);

    FILE *archivo = fopen("datos.txt", "a");
    if (archivo == NULL) {
        perror("Error al abrir el archivo");
        return NULL;
    }
    

    double d1 = t_solicita.tv_sec + t_solicita.tv_usec / 1e6;
    double d2 = t_entra.tv_sec + t_entra.tv_usec / 1e6;
    double d3 = t_sale.tv_sec + t_sale.tv_usec / 1e6;

    sem_wait(&sem_fichero);
    fprintf(archivo, "%d %d %.6f %.6f %.6f L\n", mi_nodo, contador_print_consultas, d1, d2, d3);
    sem_post(&sem_fichero);


    // Cerrar el archivo
    fclose(archivo);



    return NULL;
}

/////////////////////////////////////////////////////////////////anulacion////////////////////////////////////////////////////////////////////////////////////

void* anulacion_hilo(void* arg) {

    usleep((rand() % 50000)); // Sleep for a random time between 0 and 200 milliseconds
    int posicion;

    int contador_print_anulaciones = *((int*) arg);


    struct timeval t_solicita, t_entra, t_sale;    

    //1 tiempo que QUIERE entrar
    //2 tiempo que ENTRA
    //3 tiempo que SALE


    sem_wait(&sem_cola_anulaciones);
    posicion=cola_anulaciones++;
    sem_post(&sem_cola_anulaciones);





    gettimeofday(&t_solicita, NULL);

    printf(("entramos qaqui\n"));
    //sem_wait(&sem_max_tiquet); mi_tiquet = max_tiquet + 1; sem_post(&sem_max_tiquet);
   
   //habria q comprobar si eres el mas prioritario del nodo, por ahora obviamos
   
   sem_wait(&sem_dentro);
   if(dentro == 0) {
        sem_post(&sem_dentro);

        if(posicion == 0) {
            solicitar_seccion_critica(anulacion);
            printf("[Nodo %d] Primer anulacion, solicita sección crítica distribuida\n", mi_nodo);
            sem_wait(&sem_sc_reservas);




        } else {
            sem_wait(&sem_sc_reservas);
        }

        sem_wait(&sem_dentro);
        dentro = 1;
        sem_post(&sem_dentro);

        gettimeofday(&t_entra, NULL);

        printf("[Nodo %d] anulacion (posición %d) entra en la sección crítica\n", mi_nodo, posicion);
        sleep(tiempo_sc_anulaciones);
        printf("[Nodo %d] anulacion (posición %d) sale de la sección crítica\n", mi_nodo, posicion);
        int restantes;

        gettimeofday(&t_sale, NULL);

        sem_wait(&sem_cola_anulaciones);

        if(cola_anulaciones == 1) {//soy el ultimo
            sem_post(&sem_cola_anulaciones);
            int hay_anulacion_pendiente = hay_anulaciones_pendientes(); 
            

            if(hay_anulacion_pendiente){

                contestar_todos_replies();//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN

                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                printf("[Nodo %d] Último anulacion, libera sección crítica distribuida\n", mi_nodo);
                
                
                

                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    solicitar_seccion_critica(consulta);
                    contestar_todos_replies();


                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies();//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                }
            }
            else{//no hay anulaciones
                
                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    sem_wait(&sem_tipo_actual);
                    tipo_actual = consulta;////////////////////////si no no contesta replys
                    sem_post(&sem_tipo_actual);
                    //solicitar_seccion_critica(consulta);
                    sem_post(&sem_sc_consultas);//OJO CON ESTO
                    //contestar_todos_replies(10);


                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                    printf("[Nodo %d] Último anulacion, libera sección crítica distribuida\n", mi_nodo);
                }

            }

        } else {//no soy el ultimo
            sem_post(&sem_cola_anulaciones);
            int hay_anulacion_pendiente = hay_anulaciones_pendientes(); 

            if(hay_anulacion_pendiente){

                int por_atender = 0;
                sem_wait(&sem_max_procesos);
                maxProcesos--;
                por_atender = maxProcesos;
                sem_post(&sem_max_procesos);

                if(por_atender == 0) {//se han atendido N, recordar restablecer valor

                    liberar_seccion_critica(anulacion);//EN PARAMETRO A QUIEN CREEN Q CONTESTA
                    sem_wait(&sem_max_procesos);
                    maxProcesos = 4;
                    sem_post(&sem_max_procesos);

                    //AQUI HABRÁ Q AÑADIR COMPROBACION PRIORIDAD
                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);


                    printf("[Nodo %d] anulacion maximo, libera sección crítica distribuida\n", mi_nodo);
                
                    solicitar_seccion_critica(anulacion);

                }
                else {//no se han atendido N, no hago nada

                    /* sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro); */
                    sem_post(&sem_sc_reservas);

                }


            }
            else {//no hay anulaciones pendientes y no soy el ultimo

                sem_post(&sem_sc_reservas);
            }

        }

    } else {//hay alguien dentro
        sem_post(&sem_dentro);
        sem_wait(&sem_sc_reservas);

        gettimeofday(&t_entra, NULL);

        printf("[Nodo %d] anulacion (posición %d) entra en la sección crítica\n", mi_nodo, posicion);
        sleep(tiempo_sc_anulaciones);
        printf("[Nodo %d] anulacion (posición %d) sale de la sección crítica\n", mi_nodo, posicion);

        gettimeofday(&t_sale, NULL);


        int restantes;

        sem_wait(&sem_cola_anulaciones);

        if(cola_anulaciones == 1) {//soy el ultimo

            sem_post(&sem_cola_anulaciones);

            int hay_anulacion_pendiente = hay_anulaciones_pendientes(); 

            if(hay_anulacion_pendiente){

                contestar_todos_replies();//EN PARAMETRO A QUIEN CREEN Q CONTESTAAN

                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                printf("[Nodo %d] Último anulacion, libera sección crítica distribuida\n", mi_nodo);
            
                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    solicitar_seccion_critica(consulta);
                    contestar_todos_replies();
                    //sem_post(&sem_sc_consultas);//OJO CON ESTO

                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VAffLGA A TODOS
                }
            }
            else{//no hay anulaciones
                
                sem_wait(&sem_dentro);
                dentro = 0;
                sem_post(&sem_dentro);


                sem_wait(&sem_cola_consultas);
                if(cola_consultas > 0) {
                    sem_post(&sem_cola_consultas);
                    //solicitar_seccion_critica(consulta);
                    sem_wait(&sem_tipo_actual);
                    tipo_actual = consulta;////////////////////////si no no contesta replys
                    sem_post(&sem_tipo_actual);

                    sem_post(&sem_sc_consultas);//OJO CON ESTO
                    //contestar_todos_replies(10);//p/////////////////////////////////////////////////7ESTO FAI FALTA??????


                } else {
                    sem_post(&sem_cola_consultas);
                    contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                }

            }

        } else {//no soy el ultimo

            sem_post(&sem_cola_anulaciones);

            int hay_anulacion_pendiente = hay_anulaciones_pendientes(); 

            if(hay_anulacion_pendiente){

                int por_atender = 0;
                sem_wait(&sem_max_procesos);
                maxProcesos--;
                por_atender = maxProcesos;
                sem_post(&sem_max_procesos);

                if(por_atender == 0) {//se han atendido N, recordar restablecer valor

                    liberar_seccion_critica(anulacion);//EN PARAMETRO A QUIEN CREEN Q CONTESTA
                    sem_wait(&sem_max_procesos);
                    maxProcesos = 4;
                    sem_post(&sem_max_procesos);

                    //AQUI HABRÁ Q AÑADIR COMPROBACION PRIORIDAD
                    sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro);


                    printf("[Nodo %d] anulacion maximo, libera sección crítica distribuida\n", mi_nodo);
                    
                    solicitar_seccion_critica(anulacion);
                    //YA SE Q QUEDAN anulaciones, PIDEN ELLOS
                    /* sem_wait(&sem_cola_consultas);
                    if(cola_consultas > 0) {
                        sem_post(&sem_cola_consultas);
                        solicitar_seccion_critica(consulta);
                        sem_post(&sem_sc_reservas);//OJO CON ESTO

                    } else {
                        sem_post(&sem_cola_consultas);
                        contestar_todos_replies(10);//PUEDE Q SEA CHAPUZA, PERO METO NUM ALTO PARA Q LES VALGA A TODOS
                    } */

                }
                else {//no se han atendido N, no hago nada

                    /* sem_wait(&sem_dentro);
                    dentro = 0;
                    sem_post(&sem_dentro); */
                    sem_post(&sem_sc_reservas);

                }


            }
            else {//no hay anulaciones pendientes y no soy el ultimo

                sem_post(&sem_sc_reservas);
            }

        }


    }

    sem_wait(&sem_cola_anulaciones);
    cola_anulaciones--;
    sem_post(&sem_cola_anulaciones);
   

    FILE *archivo = fopen("datos.txt", "a");
    if (archivo == NULL) {
        perror("Error al abrir el archivo");
        return NULL;
    }
    

    double d1 = t_solicita.tv_sec + t_solicita.tv_usec / 1e6;
    double d2 = t_entra.tv_sec + t_entra.tv_usec / 1e6;
    double d3 = t_sale.tv_sec + t_sale.tv_usec / 1e6;

    sem_wait(&sem_fichero);
    fprintf(archivo, "%d %d %.6f %.6f %.6f E\n", mi_nodo, contador_print_anulaciones, d1, d2, d3);
    sem_post(&sem_fichero);

    // Cerrar el archivo
    fclose(archivo);


  

return NULL;


}
   



int main(int argc, char *argv[]) {
    int num_consultas, num_reservas, num_anulaciones;
    if(argc != 9) {
        fprintf(stderr, "Uso: %s <NUM_TOTAL_NODOS> <ID_nodo> <numConsultas> <numReservas> <numAnulaciones> <tiempo_sc_consultas> <tiempo_sc_reservas><ti\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    

    mi_nodo = atoi(argv[2]);
    num_nodos = atoi(argv[1]);
    num_consultas = atoi(argv[3]);
    num_reservas = atoi(argv[4]);
    num_anulaciones = atoi(argv[5]);
    tiempo_sc_consultas = atoi(argv[6]);
    tiempo_sc_reservas = atoi(argv[7]);
    tiempo_sc_anulaciones = atoi(argv[8]);

    

    if (mi_nodo < 1 || mi_nodo > num_nodos) {
        fprintf(stderr, "Error: Nodo no válido. Debe ser entre 1 y %d\n", num_nodos);
        exit(EXIT_FAILURE);
    }

    nodos = malloc((num_nodos - 1) * sizeof(int));
    id_nodos = malloc((num_nodos - 1) * sizeof(int));
    id_nodos_pend = malloc((num_nodos - 1) * sizeof(int));
    tipo_nodos_pend = malloc((num_nodos - 1) * sizeof(int));
    tickets_pendientes = malloc((num_nodos - 1) * sizeof(int));
    dentro_array = calloc(5, sizeof(int));  // Inicializa todo a 0

    int idx = 0;
    for (int i = 1; i <= num_nodos; i++) {
        if (i == mi_nodo) continue;
        nodos[idx++] = i;
    }

    mi_id = msgget(mi_nodo, IPC_CREAT | 0666);
    for (int i = 0; i < num_nodos - 1; i++) {
        id_nodos[i] = msgget(nodos[i], IPC_CREAT | 0666);
    }

    pthread_t hilo_receptor;
    pthread_create(&hilo_receptor, NULL, receptor, NULL);

    sem_init(&sem_quiero, 0, 1);
    sem_init(&sem_tipo_actual, 0, 1);
    sem_init(&sem_tiquet, 0, 1);
    sem_init(&sem_max_tiquet, 0, 1);
    sem_init(&sem_sc_reservas, 0, 0);
    sem_init(&sem_cola_reservas, 0, 1);
    sem_init(&sem_cola_consultas, 0, 1);
    sem_init(&sem_pend, 0, 1);
    sem_init(&sem_max_procesos, 0, 1);
    sem_init(&sem_sc_consultas, 0, 0);
    sem_init(&sem_bloqueo_consultas, 0, 0);
    sem_init(&sem_dentro, 0, 1);
    sem_init(&sem_tipo_pendiente, 0, 1);
    sem_init(&sem_respuestas_recibidas, 0, 1);
    sem_init(&sem_fichero, 0, 1);
    //sem_init(&sem_sc_administracion, 0, 1);
    sem_init(&sem_sc_anulaciones, 0, 0);
    //sem_init(&sem_sc_pagos, 0, 0);
    //sem_init(&sem_cola_administracion, 0, 1);
    sem_init(&sem_cola_anulaciones, 0, 1);
    //sem_init(&sem_cola_pagos, 0, 1);
    
    sems_sc = malloc(5 * sizeof(sem_t));
    for (int i = 0; i < 5; i++) {
        sem_init(&sems_sc[i], 0, 1);
    }
    
    
   
    
    

    pthread_t hilos[num_consultas + num_reservas + num_anulaciones];


    
    for (int i = 0; i < num_reservas; i++) {
        int* arg = malloc(sizeof(int));
        *arg = i;
        pthread_create(&hilos[num_consultas + i], NULL, reserva_hilo, arg);
    }
    
    for (int i = 0; i < num_consultas; i++) {
        int* arg = malloc(sizeof(int));
        *arg = i;
        pthread_create(&hilos[i], NULL, consulta_hilo, arg);
    }
   /*  for (int i = 0; i < num_anulaciones; i++) {
        int* arg = malloc(sizeof(int));
        *arg = i;
        pthread_create(&hilos[num_consultas + num_reservas + i], NULL, anulacion_hilo, arg);
    } */


    for (int i = 0; i < num_consultas + num_reservas + num_anulaciones; i++) pthread_join(hilos[i], NULL);
    pthread_join(hilo_receptor, NULL);





    sem_destroy(&sem_dentro);
    sem_destroy(&sem_quiero);
    sem_destroy(&sem_tipo_actual);
    sem_destroy(&sem_tiquet);
    sem_destroy(&sem_max_tiquet);
    sem_destroy(&sem_sc_reservas);
    sem_destroy(&sem_cola_reservas);
    sem_destroy(&sem_max_procesos);
    sem_destroy(&sem_cola_consultas);
    sem_destroy(&sem_pend);
    sem_destroy(&sem_sc_consultas);
    sem_destroy(&sem_bloqueo_consultas);
    sem_destroy(&sem_tipo_pendiente);
    sem_destroy(&sem_respuestas_recibidas);
    sem_destroy(&sem_fichero);
    //sem_destroy(&sem_sc_administracion);
    sem_destroy(&sem_sc_anulaciones);
    //sem_destroy(&sem_sc_pagos);
    //sem_destroy(&sem_cola_administracion);
    sem_destroy(&sem_cola_anulaciones);
    //sem_destroy(&sem_cola_pagos);
    for (int i = 0; i < 5; i++) {
        sem_destroy(&sems_sc[i]);
    }
    
    free(sems_sc);
    free(nodos);
    free(id_nodos);
    free(id_nodos_pend);
    free(tipo_nodos_pend);

    return 0;
}