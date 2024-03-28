#include <pthread.h>

// A hashmap entry stores the key, value
// and a pointer to the next entry
typedef struct ts_entry_t {
   int key;
   int value;
   struct ts_entry_t *next;
} ts_entry_t;

// A hashmap contains an array of pointers to entries,
// the capacity of the array, the size (number of entries stored), 
// and the number of operations that it has run.
typedef struct ts_hashmap_t {
   ts_entry_t **table;
   int numOps;
   int capacity;
   int size;
} ts_hashmap_t;

// function declarations
ts_hashmap_t *initmap(int);
int get(ts_hashmap_t*, int);
int put(ts_hashmap_t*, int, int);
int del(ts_hashmap_t*, int);
void printmap(ts_hashmap_t*);
void freeMap(ts_hashmap_t*);