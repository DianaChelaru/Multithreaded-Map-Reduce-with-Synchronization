#include <iostream>
#include <fstream>
#include <vector>
#include <cctype>
#include <pthread.h>
#include <unordered_map>
#include <algorithm>
#include <set>

using namespace std;

// structura pentru thread Map
struct mapStruct{
    int nrMapper;
    int nrFiles;
    int *file_index;
    vector<string> *files;
    unordered_map<string, set<int>> * mapResults;
    pthread_mutex_t *mutex;
    bool* mappers_done;
    pthread_cond_t* cond; 
    int *finished_mappers;
};

// structura pentru thread Reduce
struct reduceStruct{
    unordered_map<string, set<int>> * mapResults;
    pthread_mutex_t *mutex;
    pthread_cond_t* cond;
    bool* mappers_done;
    int nrReducer;
    char letter; 
    bool* divided;
    vector<vector<pair<string, set<int>>>> *final_results;
};

void *mapper(void* mapInfo){
    mapStruct* mapArg = (mapStruct*) mapInfo;
    int nrFiles = mapArg->nrFiles;
    int* file_index = mapArg->file_index;
    vector<string>* files = mapArg->files;
    unordered_map<string, set<int>> * mapResults = mapArg->mapResults;
    pthread_mutex_t* mutex = mapArg->mutex;
    int *finished_mappers = mapArg->finished_mappers;

    // lista partiala in care se retin cuvintele din fisierele
    // procesate de mapper
    unordered_map<string, set<int>> partialResults;

    while((*file_index) <= nrFiles - 1){
        // file_index reprezinta id-ul unui fisier
        // un mapper va procesa fisierul cu id-ul file_index urmator disponibil
        pthread_mutex_lock(mutex);

        int local_index = *file_index;
        (*file_index)++;

        pthread_mutex_unlock(mutex);

        // se deschide fisierul
        ifstream file((*files)[local_index]);
        if (!file.is_open()) {
            cout<<"Here?"<<endl;
            cerr << "Error opening the file " <<local_index;
            continue;
        }

        // si se citesc cuvintele
        string word, new_word;
        while(file >> word) {
            new_word.clear();
            for(char ch : word){
                // fiecare cuvant va contine doar litele mici
                if (isalpha(ch)){
                    if (ch <= 'Z' && ch >= 'A'){
                        ch = tolower(ch);
                        new_word += ch;
                    } else {
                        new_word += ch;
                    }
                }
            }
            // introducem cuvantul cu id-ul fisierului
            // in care a fost gasit in lista partiala
            if (!new_word.empty()) {
                partialResults[new_word].insert(local_index + 1);
            }
        }

        file.close();
    }
    pthread_mutex_lock(mutex);
    // adaugam intrarile din lista partiala in rezultatul final al mapperilor
    // un cuvant va avea o singura intrare cu toate file_id-urile in care a fost gasit
    if (!partialResults.empty()) {
        for (const auto& entry : partialResults) {
            (*mapResults)[entry.first].insert(entry.second.begin(), entry.second.end());
        }
    }
    // verificam daca s-au executat toti mapperii
    (*finished_mappers)++;
    if ((*finished_mappers) == mapArg->nrMapper) {
        // daca da, se anunta reducerii
        *(mapArg->mappers_done) = true;
        pthread_cond_broadcast(mapArg->cond);
    }
    pthread_mutex_unlock(mutex);
    
    return nullptr;
}

// functia de comparare pentru cuvintele din fisiere
bool compare(pair<string, set<int>>& a, pair<string, set<int>>& b) 
{   
    // mai intai descrescator dupa numarul id-urilor
    if (a.second.size() != b.second.size()){
        return a.second.size() > b.second.size();
    }
    // apoi dupa litere crescator
    return a.first < b.first;
}

void *reducer(void *reducerInfo){
    reduceStruct* reduceArg = (reduceStruct*) reducerInfo;
    unordered_map<string, set<int>> * mapResults = reduceArg->mapResults;
    pthread_mutex_t* mutex = reduceArg->mutex;
    char letter = reduceArg->letter;
    vector<vector<pair<string, set<int>>>> *final_results = reduceArg->final_results;
    bool *divided = reduceArg->divided;

    // se asteapta ca thread-urile Map sa termine
    pthread_mutex_lock(mutex);
    while (!*(reduceArg->mappers_done)) {
        pthread_cond_wait(reduceArg->cond, reduceArg->mutex);
    }
    // verificam daca un thread Reduce a impartit cuvintele pe litere
    if (*(divided) == false) {
        for (auto& entry : (*mapResults)) {
            (*final_results)[entry.first[0]-'a'].emplace_back(entry);
        }
        *(divided) = true;
    }
    pthread_mutex_unlock(mutex);

    // se vor procesa cuvintele care incep cu litera asignata thread-ului
    while (letter <= 'z'){
        // deschidem un fisier pentru aceasta litera
        string str(1, letter);
        ofstream file(str + ".txt");
        if (!file.is_open()) {
            cerr << "Error opening the file " << letter << ".txt\n";
                continue;
            }
            if(!(*final_results)[letter-'a'].empty()){
                // sortam cuvintele
                sort((*final_results)[letter-'a'].begin(), (*final_results)[letter-'a'].end(), compare);
                // scriem fiecare cuvant in fisier
                for(auto &entry : (*final_results)[letter-'a']){
                    file << entry.first << ":[";
                    int ct = 0;
                    for(auto &file_id : entry.second){
                        file << file_id;
                        ct++;
                        if (ct == entry.second.size()){
                            file << "]\n";
                        }else{
                            file<<" ";
                        }
                    }
                }
                file.close();
            }
        
        // thread-ul trece la litera urmatoare
        letter += reduceArg->nrReducer;
    }

    return nullptr;
}

int main(int argc, char **argv)
{
    ifstream file(argv[3]);

    if (!file.is_open()) {
        cerr << "Error opening the file " << argv[3];
        return 1;
    }

    // se citeste numarul de fisiere care trebuie procesate
    int nrFiles;
    file >> nrFiles;

    // se citesc numele fisierelor
    vector<string> files(nrFiles);
    for (int i = 0; i < nrFiles; i++){
        file >> files[i];
    }

    file.close();

    // initializam datele necesare
    int nrMapper = atoi(argv[1]);
    struct mapStruct mapArray[nrMapper];
    vector<pthread_t> map_threads;
    unordered_map<string, set<int>> mapResults;
    int file_index = 0;
    int nrReducer = atoi(argv[2]);
    struct reduceStruct reduceArray[nrReducer];
    vector<pthread_t> reduce_threads;
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, nullptr);
    pthread_cond_t cond;
    pthread_cond_init(&cond, nullptr);
    bool mappers_done = false;
    char letter = 'a';
    bool divided = false;
    vector<vector<pair<string, set<int>>>> final_results(26);
    int finished_mappers = 0;

    // se creaza thread-urile
    for(int i = 0; i < nrMapper + nrReducer; i++){
        if ( i < nrMapper){
            mapArray[i].nrMapper = nrMapper;
            mapArray[i].nrFiles = nrFiles;
            mapArray[i].file_index = &file_index;
            mapArray[i].files = &files;
            mapArray[i].mapResults = &mapResults;
            mapArray[i].mutex = &mutex;
            mapArray[i].mappers_done = &mappers_done;
            mapArray[i].cond = &cond;
            mapArray[i].finished_mappers = &finished_mappers;
            pthread_t thread;
            int pt = pthread_create(&thread, NULL, mapper, &mapArray[i]);

            if (pt) {
                cout << "Error! Could not create thread " << i << "\n";
                return 1;
            }

            map_threads.push_back(thread);
        } else {
            reduceArray[i-nrMapper].mapResults = &mapResults;
            reduceArray[i-nrMapper].mutex = &mutex;
            reduceArray[i - nrMapper].cond = &cond;
            reduceArray[i - nrMapper].mappers_done = &mappers_done;
            reduceArray[i - nrMapper].nrReducer = nrReducer;
            reduceArray[i - nrMapper].letter = letter + i - nrMapper;
            reduceArray[i - nrMapper].divided = &divided;
            reduceArray[i - nrMapper].final_results = &final_results;

            pthread_t thread;
            int pt = pthread_create(&thread, NULL, reducer, &reduceArray[i-nrMapper]);

            if (pt) {
                cout << "Error! Could not create thread " << i << "\n";
                return 1;
            }

            reduce_threads.push_back(thread);
        }
        
    }

    // se asteapta finalizarea thred-urilor
    for(int i = 0; i < nrMapper+nrReducer; i++){
        if (i < nrMapper){
            pthread_join(map_threads[i], nullptr);
        } else {
            pthread_join(reduce_threads[i-nrMapper], nullptr);
        }
    }

    // se distrug elemtele de sincronizare
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);

    return 0;
}