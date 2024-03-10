#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <sstream>
#include <queue>
#include <math.h>
#include <limits>
#include <vector>
#include <utility>
#include <algorithm>
#include <map>
#include <mpi.h>
#include <mpe.h>

#define MASTER 0
#define FROM_MASTER 1 //Envío de mensaje desde el master.
#define FROM_WORKER 2 //Envío de mensaje desde el worker.

#define NEG_INFINITY -1.0*numeric_limits<double>::max()

using namespace std;


// Define events
int BROADCAST_START, BROADCAST_END, SEND_START, SEND_END, RECEIVE_START, RECEIVE_END;

/*Variables de MPI*/
int taskId, numTasks, numWorkers, currentWorker = 0;
MPI_Status status;
MPI_Request request;
MPI_Datatype correlation_type;

/*Variables propias*/
int movies, users, recommendations;
int **utilityMatrix;
double **userUserMatrix;
int combinatorialFactor = 0; //Determinará la cantidad de valores 
//de correlación que recibirá el master.
double *userACorrs;

/*Matriz que se recibe por Broadcast en los workers*/
int **matrixUIInW;

/*Variables para almacenamiento temporal en los workers*/


struct Correlation {
    int a;
    int u;
    double correlationAu;

    Correlation() : a(0), u(0), correlationAu(0) {}

    Correlation(int userA, int userU, double corrAU) : a(userA), u(userU), correlationAu(corrAU) {}
};

queue<Correlation> corrToProc;

struct Sort {
    int a;
    vector<pair<double, int> > corrValuesA;

    Sort(int userA, vector<pair<double, int> > corrValuesUserA) : a(userA), corrValuesA(corrValuesUserA) {}
};

queue<Sort> corrToSort;

/*Vector, matriz y archivos resultado*/
int *vectorSR;
int **recommendationPerUser;
int *vectorRecMovies;
int **userPerRecommendation;
char *matrixSRFile;
char *matrixRecMovsFile;


void
printMatrixLocs() {
    printf("\n");
    printf("Recommendation matrix was output in: %s\n", matrixSRFile);
    printf("Recommended movies matrix was output in: %s\n", matrixRecMovsFile);
    printf("\n");
}
/**
 * stampa su file
 * */
void
copyToFOut() {
    string fOutSR(matrixSRFile);
    string fOutRecMov(matrixRecMovsFile);
    ofstream fileStreamOutSR(fOutSR.c_str(), ofstream::out);
    ofstream fileStreamOutRecMov(fOutRecMov.c_str(), ofstream::out);

    for (int i = 0; i < recommendations; ++i) {
        for (int j = 0; j < users; ++j) {
            fileStreamOutSR << recommendationPerUser[i][j] << " ";
        }
        fileStreamOutSR << endl;
    }

    for (int i = 0; i < users; ++i) {
        for (int j = 0; j < recommendations; ++j) {
            fileStreamOutRecMov << userPerRecommendation[i][j] << " ";
        }
        fileStreamOutRecMov << endl;
    }

    fileStreamOutSR.close();
    fileStreamOutRecMov.close();
}

void
nextWorker() {
    currentWorker++;
    if (currentWorker > numWorkers) {
        currentWorker = 1;
    }
}
/**
 * per ogni utente
 * si contattano i worker per ricevere
 * il vettore di correlazione utente-utente
 * il vettore  utente movies
 * e crea la matrice utente-movies
 * */
void
receiveRecUsersAndMoviesFromWorkers() {
    currentWorker = 0;
    nextWorker();
    int a;
    for (int i = 0; i < users; ++i) {
        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&a, 1, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
        MPE_Log_event(RECEIVE_END, 0, NULL);

        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&vectorSR[0], recommendations, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
        MPE_Log_event(RECEIVE_END, 0, NULL);
        for (int j = 0; j < recommendations; ++j) {
            //Se ordenan los usuarios más similares al usuario a en su columna.
            recommendationPerUser[j][a] = vectorSR[j];
        }
        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&vectorRecMovies[0], recommendations, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
        MPE_Log_event(RECEIVE_END, 0, NULL);

        for (int k = 0; k < recommendations; ++k) {
            //Se ordenan las películas a recomendar a ese usuario por filas para facilitar su lectura.
            userPerRecommendation[a][k] = vectorRecMovies[k];
        }
        nextWorker();
    }
    currentWorker = 0;
}

void
sortCorrelations() {
    pair<int, int> ratingAndMovie;
    while (!corrToSort.empty()) {
        Sort corrSort = corrToSort.front();
        int a = corrSort.a;
        vector<pair<double, int> > corrValuesA = corrSort.corrValuesA;
        corrToSort.pop();
        sort(corrValuesA.rbegin(), corrValuesA.rend()); //Ordena de mayor a menor
        //según el coeficiente de
        //correlación.
        int *recUsers = new int[recommendations];
        for (int i = 0; i < recommendations; ++i) {
            //Este arreglo contiene los usuarios que presentaron
            //mayor coeficiente de correlación respecto al usuario a.
            recUsers[i] = corrValuesA[i].second;
        }

        int *moviesToRecToA = new int[recommendations];
        for (int i = 0; i < recommendations; ++i) {
            vector<pair<int, int> > recUserRatings;
            for (int j = 0; j < movies; ++j) {
                ratingAndMovie = make_pair(matrixUIInW[j][recUsers[i]], j);
                recUserRatings.push_back(ratingAndMovie);
            }
            sort(recUserRatings.rbegin(), recUserRatings.rend()); //Ordena de mayor a menor
            //según los ratings dados
            //a las películas por el
            //usuario.

            bool continueRecommending = true;
            bool foundMovieToRec = false;
            int uMovieToRecA;
            for (int k = 0; (k < movies) && continueRecommending; ++k) {
                if (recUserRatings[k].first > 0) {
                    //Solo se ingresa si el usuario que recomienda tiene
                    //rating asociado a esa película.
                    int movieToRec = recUserRatings[k].second;
                    if (matrixUIInW[movieToRec][a] == 0) {
                        uMovieToRecA = movieToRec;
                        foundMovieToRec = true;
                        continueRecommending = false;
                    }
                } else {
                    continueRecommending = false;
                }
            }

            if (!foundMovieToRec) {
                uMovieToRecA = -1;
            }
            moviesToRecToA[i] = uMovieToRecA;
        }
        MPE_Log_event(SEND_START, 0, NULL);
        MPI_Isend(&a, 1, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD,&request);
        MPE_Log_event(SEND_END, 0, NULL);

        MPE_Log_event(SEND_START, 0, NULL);
        MPI_Isend(&recUsers[0], recommendations, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD,&request);
        MPE_Log_event(SEND_END, 0, NULL);

        MPE_Log_event(SEND_START, 0, NULL);
        MPI_Isend(&moviesToRecToA[0], recommendations, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD,&request);
        MPE_Log_event(SEND_END, 0, NULL);

    }
}
/**
 *
 * riceve le correlazionie
 * potrebbe fare un inserimento ordinato e ordinarle direttamente qua
 * aumenterebbe la concorrenza
 * */
void
receiveCorrColumnsFromMaster() {
    int a; //Índice del usuario base.
    double *userACorrValues;
    pair<double, int> corrAndUser;

    int n_receive = users / numWorkers;
    if(taskId<=users % numWorkers){
        n_receive++;
    }
    MPI_Request a_request;
    for(int j=0; j<n_receive; j++){
        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Irecv(&a, 1, MPI_INT, MASTER, FROM_MASTER, MPI_COMM_WORLD, &a_request);
        MPE_Log_event(RECEIVE_END, 0, NULL);
        vector<pair<double, int> > corrsVector;
        userACorrValues = new double[users];
        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&userACorrValues[0], users, MPI_DOUBLE, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);
        MPE_Log_event(RECEIVE_END, 0, NULL);

        for (int i = 0; i < users; ++i) {
            //Se guarda el coeficiente de correlación acompañado de su usuario asociado.
            corrAndUser = make_pair(userACorrValues[i], i);
            corrsVector.push_back(corrAndUser);
        }

        MPI_Wait(&a_request, MPI_STATUS_IGNORE);
        Sort corrSort = Sort(a, corrsVector);
        corrToSort.push(corrSort);

    }
}
/**per ogni utente invia ad anello
 * ai worker l'utente  e le sue correlazioni con gli altri
 * finisce lo stream con -1
 * */
void
sendCorrColumnsToWorkers() {
    currentWorker = 0;
    nextWorker();
    int a;
    for (int i = 0; i < users; ++i) {
        a = i;  //Índice del usuario a.
        MPE_Log_event(SEND_START, 0, NULL);
        MPI_Isend(&a, 1, MPI_INT, currentWorker, FROM_MASTER, MPI_COMM_WORLD,&request);
        MPE_Log_event(SEND_END, 0, NULL);

        for (int j = 0; j < users; ++j) {
            userACorrs[j] = userUserMatrix[j][a];
        }
        MPE_Log_event(SEND_START, 0, NULL);
        MPI_Isend(&userACorrs[0], users, MPI_DOUBLE, currentWorker, FROM_MASTER, MPI_COMM_WORLD,&request);
        MPE_Log_event(SEND_END, 0, NULL);

        nextWorker();
    }

    currentWorker = 0;
}
/**
 * il master riceve n^2/2
 * riceve in ordine i valori a,u, e la correlazione
 * si puo rendere il tutto asincrono e fare una riceive bloccante
 * con un tipo piu complesso che ha user a,u e correlazione
 * */
void
receiveCorrValuesFromWorkers() {
    currentWorker = 0;
    nextWorker();
    int a, u;
    double corrValue;
    for (int i = 0; i < combinatorialFactor; ++i) {

        //RICEZIONE DI UN SOLO MSG CON IL TIPO COMPOSTO DA INT, INT E DOUBLE
        Correlation correlation;

        MPE_Log_event(SEND_START, 0, NULL);
        MPI_Recv(&correlation, 1, correlation_type, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
        MPE_Log_event(SEND_END, 0, NULL);

        /*
        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&a, 1, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
        MPE_Log_event(RECEIVE_END, 0, NULL);

        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&u, 1, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
        MPE_Log_event(RECEIVE_END, 0, NULL);

        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&corrValue, 1, MPI_DOUBLE, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
        MPE_Log_event(RECEIVE_END, 0, NULL);
        */

        userUserMatrix[correlation.a][correlation.u] = correlation.correlationAu;
        userUserMatrix[correlation.u][correlation.a] = correlation.correlationAu;
        //printf("Corr (%d, %d) = %f\n", a, u, corrValue);
        nextWorker();
    }
    currentWorker = 0;
}

/**
 * Potrebbe essere chiamato nella receive indexes
 * */
void
processCorrelations() {
    while (!corrToProc.empty()) {
        Correlation correlation = corrToProc.front();
        corrToProc.pop();
        //INVIO DI UN SOLO MSG CON IL TIPO COMPOSTO DA INT, INT E DOUBLE
        MPE_Log_event(SEND_START, 0, NULL);
        MPI_Send(&correlation, 1, correlation_type, MASTER, FROM_WORKER, MPI_COMM_WORLD);
        MPE_Log_event(SEND_END, 0, NULL);

    }
}

void
processNewCorrelations(Correlation* correlation, double* averages, double* corrTerms, int*userARatings, int* userURatings, map<int, int *>*usersRatings) {
        int a = correlation->a;
        int u = correlation->u;

        if (usersRatings->count(a)) {
            userARatings = (*usersRatings)[a];
        } else {
            for (int i = 0; i < movies; ++i) {
                userARatings[i] = matrixUIInW[i][a];
            }
            (*usersRatings)[a] = userARatings;
        }

        if (usersRatings->count(u)) {
            userURatings = (*usersRatings)[u];
        } else {
            for (int i = 0; i < movies; ++i) {
                userURatings[i] = matrixUIInW[i][u];
            }
            (*usersRatings)[u] = userURatings;
        }

        //Encontrar ratings promedio del usuario a.
        double aAvg = 0.0;
        if (averages[a] == -1.0) {
            double contRatA = 0.0;
            for (int i = 0; i < movies; ++i) {
                if (userARatings[i] != 0) {
                    aAvg += userARatings[i];
                    contRatA += 1.0;
                }
            }
            aAvg = aAvg / contRatA;
            averages[a] = aAvg;
        } else {
            aAvg = averages[a];
        }

        //Encontrar ratings promedio del usuario u.
        double uAvg = 0.0;
        if (averages[u] == -1.0) {
            double contRatU = 0.0;
            for (int j = 0; j < movies; ++j) {
                if (userURatings[j] != 0) {
                    uAvg += userURatings[j];
                    contRatU += 1.0;
                }
            }
            uAvg = uAvg / contRatU;
            averages[u] = uAvg;
        } else {
            uAvg = averages[u];
        }

        //Calcular el primer término del denominador.
        double denomFirstTerm = 0.0;
        if (corrTerms[a] == -1.0) {
            for (int i = 0; i < movies; ++i) {
                if (userARatings[i] != 0) {
                    denomFirstTerm += pow((userARatings[i] - aAvg), 2);
                }
            }
            denomFirstTerm = sqrt(denomFirstTerm);
            corrTerms[a] = denomFirstTerm; //Se almacena ese término para no tener
            //que volverlo a calcular.
        } else {
            denomFirstTerm = corrTerms[a];
        }

        //Calcular el segundo término del denominador.
        double denomSecondTerm = 0.0;
        if (corrTerms[u] == -1.0) {
            for (int j = 0; j < movies; ++j) {
                if (userURatings[j] != 0) {
                    denomSecondTerm += pow((userURatings[j] - uAvg), 2);
                }
            }
            denomSecondTerm = sqrt(denomSecondTerm);
            corrTerms[u] = denomSecondTerm; //Se almacena ese término para no tener
            //que volverlo a calcular.
        } else {
            denomSecondTerm = corrTerms[u];
        }

        //Calcular el denominador completo.
        double denominator = denomFirstTerm * denomSecondTerm;

        //Calular el numerador completo.
        double numerator = 0.0;
        for (int i = 0; i < movies; ++i) {
            //Solamente se incluirán aquellas películas que ambos hayan visto.
            if (userARatings[i] != 0 && userURatings[i] != 0) {
                numerator += ((userARatings[i] - aAvg) * (userURatings[i] - uAvg));
            }
        }

        //Calcular el valor de correlación entre ambos usuarios.
        double corrBtwAandU = numerator / denominator;

        correlation->correlationAu = corrBtwAandU;
    }
/**
 * riceve coppie fino alla fine dello stream
 * e tra una receive e l'altra crea oggetto correlazione ma non calcola
 * la potrebbe calcolare qua
 * */
void
receiveUsersIndexesFromMaster() {
    int n_pairs = (users* (users-1))/2;
    int n_receive = n_pairs/numWorkers;
    if(taskId<= (n_pairs)%numWorkers){
        n_receive++;
    }

    double *averages = new double[users];
    double *corrTerms = new double[users];
    //Se inicializan los vectores que usarán los workers para poder
    //enviar los coeficientes de correlación entre los usuarios.
    int *userARatings = new int[movies];
    int *userURatings = new int[movies];

    for (int i = 0; i < users; ++i) {
        //-1.0 en una posición indica que aún no se han calculado
        //ni el promedio ni los términos para la correlación.
        averages[i] = -1.0;
        corrTerms[i] = -1.0;
    }

    map<int, int *> usersRatings;

    for(int j=0; j<n_receive; j++){
        Correlation correlation;
        MPE_Log_event(RECEIVE_START, 0, NULL);
        MPI_Recv(&correlation, 1, correlation_type, MASTER, FROM_MASTER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPE_Log_event(RECEIVE_END, 0, NULL);

        processNewCorrelations(
                &correlation,
                averages,
                corrTerms,
                userARatings,
                userURatings,
                &usersRatings
        );

        corrToProc.push(correlation);
    }
}
/**
 * Manda tutte le possibili coppie di utenti
 * a tutti per calcolare
 * n^2/2
 * i worker sono ad anello gli vengono assegnati i vari valori
 * con -1 finisce lo stream
 * potrebbero essere non bloccanti?
 *
 * */
void
sendUsersIndexesToWorkers() {
    currentWorker = 0;
    nextWorker();

    int a, u;
    for (int i = 0; i < users; ++i) {
        for (int j = i + 1; j < users; ++j) {
            Correlation correlation = Correlation(i,j,0);
            MPE_Log_event(SEND_START, 0, NULL);
            MPI_Isend(&correlation, 1, correlation_type, currentWorker, FROM_MASTER, MPI_COMM_WORLD,&request);
            MPE_Log_event(SEND_END, 0, NULL);

            combinatorialFactor++;
            nextWorker();
        }
    }
    currentWorker = 0;
}

void
receiveDataFromMaster() {
    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&movies, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);

    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&users, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);

    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&recommendations, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);


    matrixUIInW = new int *[movies];
    for (int i = 0; i < movies; ++i) {
        matrixUIInW[i] = new int[users];
    }
    int *linearMatrixUIInW;
    linearMatrixUIInW = new int[movies * users];
    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&linearMatrixUIInW[0], movies * users, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);

    for (int i = 0; i < movies; ++i) {
        for (int j = 0; j < users; ++j) {
            matrixUIInW[i][j] = linearMatrixUIInW[(users * i) + j];
        }
    }
}

void
sendDataToWorkers() {
    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&movies, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);

    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&users, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);

    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&recommendations, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);


    int *linearMatrixUI = new int[movies * users];
    for (int i = 0; i < movies; ++i) {
        for (int j = 0; j < users; ++j) {
            linearMatrixUI[(users * i) + j] = utilityMatrix[i][j];
        }
    }
    MPE_Log_event(BROADCAST_START, 0, NULL);
    MPI_Bcast(&linearMatrixUI[0], movies * users, MPI_INT, MASTER, MPI_COMM_WORLD);
    MPE_Log_event(BROADCAST_END, 0, NULL);

}

void
openFiles(char *fileNameMatrixUI) {
    string fNameMatUI(fileNameMatrixUI);
    ifstream fileStreamMatUI(fNameMatUI.c_str());

    int i = 0;  //Índice de la fila (movie).
    int j = -1; //Índice de la columna (user).
    if (fileStreamMatUI.is_open()) {
        string movieRow;
        while (getline(fileStreamMatUI, movieRow)) {
            stringstream ssFileStreamMatUI(movieRow);
            string rating;
            while (getline(ssFileStreamMatUI, rating, ' ')) {
                j++;
                utilityMatrix[i][j] = atoi(rating.c_str());
            }
            j = -1;
            i++;
        }
        fileStreamMatUI.close();
    } else {
        printf("Unable to open %s\n", fileNameMatrixUI);
    }
}

void
initMatricesAndResponseMat() {
    utilityMatrix = new int *[movies];
    for (int i = 0; i < movies; ++i) {
        utilityMatrix[i] = new int[users];
    }

    userUserMatrix = new double *[users];
    for (int i = 0; i < users; ++i) {
        userUserMatrix[i] = new double[users];
    }

    //Se hace para asegurarse de que en la diagonal queden valores -infinitos.
    for (int j = 0; j < users; ++j) {
        userUserMatrix[j][j] = NEG_INFINITY;
    }

    userACorrs = new double[users];

    vectorSR = new int[recommendations];

    recommendationPerUser = new int *[recommendations];
    for (int i = 0; i < recommendations; ++i) {
        recommendationPerUser[i] = new int[users];
    }

    vectorRecMovies = new int[recommendations];

    userPerRecommendation = new int *[users];
    for (int i = 0; i < users; ++i) {
        userPerRecommendation[i] = new int[recommendations];
    }
}

void
initMPI(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    MPE_Init_log();


    MPI_Comm_rank(MPI_COMM_WORLD, &taskId);
    //numTasks corresponde al parámetro que acompaña -np X.
    MPI_Comm_size(MPI_COMM_WORLD, &numTasks);
    numWorkers = numTasks - 1;

    BROADCAST_START = MPE_Log_get_event_number();
    BROADCAST_END = MPE_Log_get_event_number();
    SEND_START = MPE_Log_get_event_number();
    SEND_END = MPE_Log_get_event_number();
    RECEIVE_START = MPE_Log_get_event_number();
    RECEIVE_END = MPE_Log_get_event_number();

    if (taskId == 0) {
        MPE_Describe_state(BROADCAST_START, BROADCAST_END, "Broadcast", "red:vlines3");
        MPE_Describe_state(SEND_START, SEND_END, "Send", "blue:gray3");
        MPE_Describe_state(RECEIVE_START, RECEIVE_END, "Receive", "green:light_gray");
    }
}

//argv 1 - filename_in.
//argv 2 - filename_out_SR.
//argv 3 - filepath_out_RecMovs.
//argv 4 - movies.
//argv 5 - users.
//argv 6 - recommendations.

void define_mpi_data_types() {
    // Create the datatype
    int lengths[3] = { 1, 1, 1};
    MPI_Aint displacements[3];
    Correlation dummy_correlation;
    MPI_Aint base_address;
    MPI_Get_address(&dummy_correlation, &base_address);
    MPI_Get_address(&dummy_correlation.a, &displacements[0]);
    MPI_Get_address(&dummy_correlation.u, &displacements[1]);
    MPI_Get_address(&dummy_correlation.correlationAu, &displacements[2]);
    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);
    displacements[2] = MPI_Aint_diff(displacements[2], base_address);

    MPI_Datatype types[3] = { MPI_INT, MPI_INT, MPI_DOUBLE };
    MPI_Type_create_struct(3, lengths, displacements, types, &correlation_type);
    MPI_Type_commit(&correlation_type);

}

int
main(int argc, char *argv[]) {

    initMPI(argc, argv);
    define_mpi_data_types();
    double startTime = MPI_Wtime();

    if (taskId == MASTER) {
        if (argc != 8) {
            printf("USAGE: recommendationSystem [matrixUI_file_name] [matrixSR_file_name] [matrixRecMovs_file_name] ");
            printf("[number_of_movies] [number_of_users] [number_of_recommendations_wanted]\n");
            MPE_Finish_log(argv[7]);
            MPI_Finalize();
            return 0;
        }

        if (atoi(argv[6]) > atoi(argv[4])) {
            printf("Movies to recommend for each user should be less than or equals to %d.\n", atoi(argv[4]));
            MPE_Finish_log(argv[7]);
            MPI_Finalize();
            return 0;
        }

        //Iniciamos cronómetro.

        matrixSRFile = argv[2];
        matrixRecMovsFile = argv[3];
        movies = atoi(argv[4]);
        users = atoi(argv[5]);
        recommendations = atoi(argv[6]);

        printf("Processing...\n");
        //Inicializar las películas y usuarios de la matrizUI.
        initMatricesAndResponseMat();

        //Abrir el archivo que contiene utilityMatrix.
        openFiles(argv[1]);

        //Enviar a los workers los datos de movies y users.
        sendDataToWorkers();

        //Enviar las columnas de utilityMatrix a los workers.
        sendUsersIndexesToWorkers();

        /*En este punto ya los workers empezaron a procesar y a enviar resultados parciales
          referentes a los valores para la matriz de correlación
        */

        //Recibir las respuestas parciales desde los workers.
        receiveCorrValuesFromWorkers();

        //Enviar las columnas de correlación por usuarios a los workers.
        sendCorrColumnsToWorkers();

        //Recibir películas recomendadas para cada usuario desde los workers.
        receiveRecUsersAndMoviesFromWorkers();

        //Imprimir resultado como matriz.
        printMatrixLocs();

        //Imprimir resultado en el archivo de salida.
        copyToFOut();

    } else {

        if (argc != 8) {
            MPE_Finish_log(argv[7]);
            MPI_Finalize();
            return 0;
        }

        if (atoi(argv[6]) > atoi(argv[4])) {
            MPE_Finish_log(argv[7]);
            MPI_Finalize();
            return 0;
        }

        //Recibir del master los datos de movies y users.
        receiveDataFromMaster();

        //Recibir las columnas de las matrices desde el master.
        receiveUsersIndexesFromMaster();

        //Procesar los registros de la cola de correlaciones para enviárselos al master.
        processCorrelations();

        //Recibir las columnas de correlación por usuarios desde el master.
        receiveCorrColumnsFromMaster();

        //Ordenar los coeficientes de correlación presentes en la cola de sorts para enviárselos al master.
        sortCorrelations();
    }

    //Paramos el cronómetro.
    double endTime = MPI_Wtime();
    MPE_Finish_log(argv[7]);
    MPI_Finalize();

    if(taskId == MASTER){
        printf("Total execution time: %f s.\n", endTime - startTime);

    }
    return 0;
}


