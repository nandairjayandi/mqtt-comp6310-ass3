#ifndef LOGGER_H
#define LOGGER_H

#include <stddef.h> 
#include <curl/curl.h>

#define LOGGER_BUF_SIZE 2048 // number of log entries before dumping to a tsv file
#define LOGGER_TOPIC_MAX 64 // Max topic string length — counter/2/100/4000 = 20 chars i.e. 64 is good enough
#define LOGGER_DIR_MAX 256 // length of directory path

#define DEFAULT_SAMPLE_RATE 100

/*
 * represents one row in tsv file
 */
typedef struct {
    long long   counter; // message sequence number from publisher (0, 1, 2, ...)
    long long   pub_ts; // Unix epoch ms when publisher sent the message
    long long   recv_ts; // Unix epoch ms when analyser mqtt callback fired
    int         latency_ms; // recv - pub timestamp
    char        topic[LOGGER_TOPIC_MAX]; // mqtt topic string e.g. "counter/0/0/1000"
    int         msg_size; // declared payload x-string length (1, 1000, or 4000)
} log_entry_t;

/*
 * represents the logger state
 */
typedef struct {
    log_entry_t buf[LOGGER_BUF_SIZE]; // in-memory buffer              
    int         count; // entries currently in buffer   
    char        output_dir[LOGGER_DIR_MAX]; // directory to write files 
    
    int         pub_qos;
    int         sub_qos;
    int         delay_ms;
    int         msg_size;

    // for influxdb monitoring
    CURL        *curl;
    char        influx_url[256];
    char        influx_token[128];
    char        influx_org[64];
    char        influx_bucket[64];
    char        influx_buffer[65536];
    size_t      influx_buffer_pos;

    int         influx_sample_rate; 
} logger_t;


int logger_init(logger_t *log, const char *output_dir);
void logger_set_test(logger_t *log, int pub_qos, int sub_qos, int delay_ms, int msg_size);
int logger_write(logger_t *log, long long counter, long long pub_ts, long long recv_ts, const char *topic, int msg_size);
int logger_flush(logger_t *log);
void logger_close(logger_t *log); 

int logger_init_influx(logger_t *log, const char *url, const char *token, const char *org, const char *bucket);
int influx_flush(logger_t *log);  

int write_ts_meta(const logger_t *log, long long start_ts, long long end_ts);

#endif