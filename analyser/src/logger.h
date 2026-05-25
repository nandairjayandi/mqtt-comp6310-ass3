#ifndef LOGGER_H
#define LOGGER_H

#include <stddef.h> 

#define LOGGER_BUF_SIZE 2048 // number of log entries before dumping to a tsv file
#define LOGGER_TOPIC_MAX 64 // Max topic string length — counter/2/100/4000 = 20 chars i.e. 64 is good enough
#define LOGGER_DIR_MAX 256 // length of directory path

/*
 * represents one row in tsv file
 */
typedef struct {
    long long   counter; // message sequence number from publisher (0, 1, 2, ...)
    long long   pub_timestamp; // Unix epoch ms when publisher sent the message
    long long   recv_timestamp; // Unix epoch ms when analyser mqtt callback fired
    char        topic[LOGGER_TOPIC_MAX]; // mqtt topic string e.g. "counter/0/0/1000"
    int         msg_size; // declared payload x-string length (1, 1000, or 4000)
} log_entry_t;

/*
 * represents the logger state
 */
typedef struct {
    log_entry_t buf[LOGGER_BUF_SIZE]; // in-memory buffer              
    int         count; // entries currently in buffer   
    long long   start_counter; // counter value of buf[0]      
    char        output_dir[LOGGER_DIR_MAX]; // directory to write files 
} logger_t;


int logger_init(logger_t *log, const char *output_dir);
int logger_write(logger_t *log, long long counter, long long pub_timestamp, long long recv_timestamp, const char *topic, int msg_size);
int logger_flush(logger_t *log);

#endif