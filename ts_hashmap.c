#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap. 
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity) {
  // Instantiate fields
  ts_hashmap_t *map = (ts_hashmap_t*) malloc(sizeof(ts_hashmap_t));
  map->numOps = 0;
  map->capacity = capacity;
  map->size = 0;

  // Initialize locks
  pthread_mutex_t *opLock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
  pthread_mutex_t *sizeLock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(opLock, NULL);
  pthread_mutex_init(sizeLock, NULL);

  // Initialize arrays
  ts_entry_t **table = (ts_entry_t**) malloc(capacity * sizeof(ts_entry_t*));
  pthread_mutex_t **bucketLocks = (pthread_mutex_t**) malloc(capacity * sizeof(pthread_mutex_t*));

  // Populate arrays
  for (int i = 0; i < capacity; i++){
    table[i] = NULL;  // Hashmap starts empty
    bucketLocks[i] = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(bucketLocks[i], NULL);
  }

  // Assign lock and array pointers to map
  map->table = table;
  map->bucketLocks = bucketLocks;
  map->opLock = opLock;
  map->sizeLock = sizeLock;

  // Return map pointer
  return map;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key) {
  unsigned int searchIndex = ((unsigned int) key) % map->capacity;
  int returnValue = INT_MAX;
  
  // Enter critical section
  pthread_mutex_lock(map->bucketLocks[searchIndex]);
  
  ts_entry_t *bucket = map->table[searchIndex];
  while (bucket != NULL){
    if (key == bucket->key){
      // found value
      returnValue = bucket->value;
      break;
    }
    // Check next bucket
  bucket = bucket->next;
  }
  
  // Exit critical section
  pthread_mutex_unlock(map->bucketLocks[searchIndex]);

  // Increment op counter
  pthread_mutex_lock(map->opLock);
  map->numOps ++;
  pthread_mutex_unlock(map->opLock);
  
  // Return value
  return returnValue;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value) {
  unsigned int searchIndex = ((unsigned int) key) % map->capacity;
  int returnValue = INT_MAX;
  int madeNewBucket = 0;
  
  // Enter critical section
  pthread_mutex_lock(map->bucketLocks[searchIndex]);
  
  ts_entry_t *bucket = map->table[searchIndex];

  // Edge case: entry is null
  if (bucket == NULL) {
    // malloc new entry
      madeNewBucket = 1;
      ts_entry_t *newBucket = (ts_entry_t*) malloc(sizeof(ts_entry_t));

      // assign newBucket attributes
      newBucket->key = key;
      newBucket->value = value;
      newBucket->next = NULL;

      // add to map
      map->table[searchIndex] = newBucket; 
  } else {
    while (bucket != NULL){
      if (key == bucket->key){
        // found key; store old value for returning and replace with new value
        returnValue = bucket->value;
        bucket->value = value;
        break;
      }
      // check if no more buckets
      if (bucket->next == NULL){
        // malloc new entry
        madeNewBucket = 1;
        ts_entry_t *newBucket = (ts_entry_t*) malloc(sizeof(ts_entry_t));

        // assign newBucket attributes
        newBucket->key = key;
        newBucket->value = value;
        newBucket->next = NULL;

        // add to map
        bucket->next = newBucket;
        break;
      } 

      // else
      bucket = bucket->next;
    }
  }
  // Exit critical section
  pthread_mutex_unlock(map->bucketLocks[searchIndex]);

  if (madeNewBucket){
    // Increment size
    pthread_mutex_lock(map->sizeLock);
    map->size ++;
    pthread_mutex_unlock(map->sizeLock);
  }

  // Increment op counter
  pthread_mutex_lock(map->opLock);
  map->numOps ++;
  pthread_mutex_unlock(map->opLock);
  
  // Return value
  return returnValue;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key) {
  unsigned int searchIndex = ((unsigned int) key) % map->capacity;
  int returnValue = INT_MAX;

  // Increment op counter
  pthread_mutex_lock(map->opLock);
  map->numOps ++;
  pthread_mutex_unlock(map->opLock);
  
  // Enter critical section
  pthread_mutex_lock(map->bucketLocks[searchIndex]);

  ts_entry_t *bucket = map->table[searchIndex];

  // Edge case: bucket is null
  if(bucket == NULL){
    pthread_mutex_unlock(map->bucketLocks[searchIndex]);
    return returnValue;
  }

  // Edge case: it's the first entry in the chain
  if (bucket->key == key){
    returnValue = bucket->value;

    if (bucket->next == NULL){
      // Trivial: only entry in chain
      free(bucket);
      map->table[searchIndex] = NULL;
    } else {
      // There is a child
      map->table[searchIndex] = bucket->next;
      free(bucket);
    }
    
    // Exit critical section
    pthread_mutex_unlock(map->bucketLocks[searchIndex]);

    // Decrement size
    pthread_mutex_lock(map->sizeLock);
    map->size --;
    pthread_mutex_unlock(map->sizeLock);

    return returnValue;
  }
  int bucketWasDeleted = 0;
  
  // Look one child ahead
  while (bucket->next != NULL){ // Is this the loop I want?

    if (key == bucket->next->key){
      // next entry is one to be deleted
      returnValue = bucket->next->value;
      ts_entry_t *deleteMe = bucket->next;
      bucket->next = bucket->next->next;

      free(deleteMe);
      bucketWasDeleted = 1;
      break;
    }
    
    bucket = bucket->next;
  }

  if (bucketWasDeleted){
    // Decrement size
    pthread_mutex_lock(map->sizeLock);
    map->size --;
    pthread_mutex_unlock(map->sizeLock);
  }
  
  // Exit critical section
  pthread_mutex_unlock(map->bucketLocks[searchIndex]);
  
  return returnValue;
}


/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map) {
  for (int i = 0; i < map->capacity; i++) {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL) {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map) {
  // free each chain of entries and their corresponding lock
  for (int i = 0; i < map->capacity; i++){
    if (map->table[i] != NULL){
      freeChain(map->table[i]);
    }
    pthread_mutex_destroy(map->bucketLocks[i]);
    free(map->bucketLocks[i]);
  }

  // free attributes of map
  free(map->table);
  free(map->bucketLocks);
  pthread_mutex_destroy(map->opLock);
  pthread_mutex_destroy(map->sizeLock);
  free(map->opLock);
  free(map->sizeLock);
  
  // free hash table
  free(map);
}

/**
 * Free up a chain of entries
 * @param entry the head of a chain of entries
*/
void freeChain(ts_entry_t *entry) {
  if (entry->next != NULL){
    freeChain(entry->next);
  }
  free(entry);
}