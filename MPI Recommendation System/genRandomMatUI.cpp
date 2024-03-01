#include <stdio.h>
#include <stdlib.h>
#include <time.h>

void generaMatrice(int righe, int colonne, const char *nomeFile) {
    // Apri il file in modalità scrittura
    FILE *file = fopen(nomeFile, "w");

    // Verifica se il file è stato aperto correttamente
    if (file == NULL) {
        fprintf(stderr, "Errore nell'apertura del file.\n");
        exit(EXIT_FAILURE);
    }

    // Inizializza il generatore di numeri casuali con il tempo attuale
    srand((unsigned int)time(NULL));

    // Genera e salva la matrice
    for (int i = 0; i < righe; i++) {
        for (int j = 0; j < colonne; j++) {
            int valore = rand() % 6; // Numeri casuali compresi tra 0 e 99
            fprintf(file, "%d ", valore);
        }
        fprintf(file, "\n");
    }

    // Chiudi il file
    fclose(file);
}

int main(int argc, char *argv[]) {
    // Verifica che siano stati passati 3 argomenti da riga di comando
    if (argc != 4) {
        fprintf(stderr, "Utilizzo: %s <numero righe> <numero colonne> <nome file>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // Ottieni il numero di righe, colonne e il nome del file dai parametri
    int righe = atoi(argv[1]);
    int colonne = atoi(argv[2]);
    const char *nomeFile = argv[3];

    // Verifica che il numero di righe e colonne sia positivo
    if (righe <= 0 || colonne <= 0) {
        fprintf(stderr, "Il numero di righe e colonne deve essere positivo.\n");
        exit(EXIT_FAILURE);
    }

    // Chiama la funzione per generare e salvare la matrice
    generaMatrice(righe, colonne, nomeFile);

    printf("Matrice generata e salvata con successo nel file %s.\n", nomeFile);

    return 0;
}