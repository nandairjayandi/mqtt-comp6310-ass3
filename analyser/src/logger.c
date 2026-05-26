#include "logger.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

extern long long get_timestamp_ms(void);
extern void timestamp_to_iso(long long ms, char *buf, size_t buf_size);

static void build_filepath(const logger_t *log, char *path, size_t path_size) {
    snprintf(
        path, path_size, 
        "%s/pq%d_sq%d_d%d_s%d.tsv",
        log->output_dir,
        log->pub_qos, 
        log->sub_qos,
        log->delay_ms, 
        log->msg_size
    );
}

static int dump(logger_t *log) {
    if (log->count == 0) { return 0; }

    char path[LOGGER_DIR_MAX + 64];
    build_filepath(log, path, sizeof(path));

    FILE *fp = fopen(path, "a");
    if (fp == NULL) {
        fprintf(stderr, "logger: failed to open '%s': %s\n", path, strerror(errno));
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    if (ftell(fp) == 0) {
        fprintf(fp, "counter\tpub_timestamp\trecv_timestamp\tlatency_ms\ttopic\tmsg_size\tpub_qos\tsub_qos\tdelay_ms\n");
    }

    int write_error = 0;
    for (int i = 0; i < log->count; i++) {
        const log_entry_t *e = &log->buf[i];
        int ret = fprintf(fp, "%lld\t%lld\t%lld\t%d\t%s\t%d\t%d\t%d\t%d\n",
            e->counter,
            e->pub_timestamp,
            e->recv_timestamp,
            e->latency_ms,
            e->topic,
            e->msg_size,
            log->pub_qos,
            log->sub_qos,
            log->delay_ms
        );

        if (ret < 0) {
            fprintf(stderr, "logger: write error at entry %d\n", i);
            write_error = 1;
            break;
        }
    }

    fclose(fp);
    log->count = 0;
    return write_error ? -1 : 0;
}

static int influx_send_point(logger_t *log, long long pub_timestamp, int latency_ms) {
    if (!log->curl) return 0;  // InfluxDB not initialized
    
    char line[256];
    snprintf(line, sizeof(line),
        "mqtt_latency,"
        "pub_qos=%d,sub_qos=%d,delay_ms=%d,msg_size=%d "
        "latency_ms=%di,msg_count=1i "    // ← count every message
        "%lld\n",
        log->pub_qos, log->sub_qos, log->delay_ms, log->msg_size,
        latency_ms,
        pub_timestamp * 1000000LL
    );
        
    size_t len = strlen(line);
    if (log->influx_buffer_pos + len >= sizeof(log->influx_buffer)) {
        influx_flush(log);
    }
    memcpy(log->influx_buffer + log->influx_buffer_pos, line, len);
    log->influx_buffer_pos += len;
    return 0;
}

int influx_flush(logger_t *log) {
    if (!log->curl || log->influx_buffer_pos == 0) return 0;
    
    char url[512];
    snprintf(url, sizeof(url), 
             "%s/api/v2/write?bucket=%s&org=%s",
             log->influx_url, log->influx_bucket, log->influx_org);
    
    struct curl_slist *headers = NULL;
    char auth_header[256];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Token %s", log->influx_token);
    headers = curl_slist_append(headers, auth_header);
    headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");
    
    curl_easy_setopt(log->curl, CURLOPT_URL, url);
    curl_easy_setopt(log->curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(log->curl, CURLOPT_POSTFIELDS, log->influx_buffer);
    curl_easy_setopt(log->curl, CURLOPT_POSTFIELDSIZE, log->influx_buffer_pos);
    
    CURLcode res = curl_easy_perform(log->curl);
    if (res != CURLE_OK) {
        fprintf(stderr, "InfluxDB write failed: %s\n", curl_easy_strerror(res));
    }
    
    curl_slist_free_all(headers);
    log->influx_buffer_pos = 0;
    return (res == CURLE_OK) ? 0 : -1;
}

void logger_set_test(logger_t *log, int pub_qos, int sub_qos, int delay_ms, int msg_size) {
    // flush any leftover data from the previous test before switching
    if (log->count > 0) dump(log);
 
    log->pub_qos = pub_qos;
    log->sub_qos = sub_qos;
    log->delay_ms = delay_ms;
    log->msg_size = msg_size;
    log->count = 0;
}

int logger_init(logger_t *log, const char *output_dir) {
    memset(log, 0, sizeof(logger_t));
    strncpy(log->output_dir, output_dir, LOGGER_DIR_MAX - 1);
    
    // Initialize curl to NULL (will be set by logger_init_influx if called)
    log->curl = NULL;
    log->influx_buffer_pos = 0;

    if (mkdir(output_dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "logger: failed to create directory '%s': %s\n", output_dir, strerror(errno));
        return -1;
    }
    return 0;
}

int logger_init_influx(logger_t *log, const char *url, const char *token,
                       const char *org, const char *bucket) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    log->curl = curl_easy_init();
    if (!log->curl) return -1;
    
    strncpy(log->influx_url, url, sizeof(log->influx_url)-1);
    strncpy(log->influx_token, token, sizeof(log->influx_token)-1);
    strncpy(log->influx_org, org, sizeof(log->influx_org)-1);
    strncpy(log->influx_bucket, bucket, sizeof(log->influx_bucket)-1);
    log->influx_buffer_pos = 0;
    
    printf("logger: InfluxDB initialized at %s\n", url);
    return 0;
}

int logger_write(logger_t *log, long long counter, long long pub_timestamp, long long recv_timestamp, const char *topic, int msg_size) {
    log_entry_t *e = &log->buf[log->count];

    e->counter = counter;
    e->pub_timestamp = pub_timestamp;
    e->recv_timestamp = recv_timestamp;
    e->latency_ms = (int)(recv_timestamp - pub_timestamp);
    strncpy(e->topic, topic, LOGGER_TOPIC_MAX - 1);
    e->topic[LOGGER_TOPIC_MAX - 1] = '\0';
    e->msg_size = msg_size;

    log->count++;
    
    if (log->curl) {
        influx_send_point(log, pub_timestamp, e->latency_ms);
    }
    
    // Flush TSV buffer when full
    if (log->count >= LOGGER_BUF_SIZE) {
        return dump(log);
    }
    return 0;
}

int logger_flush(logger_t *log) {
    // Flush both TSV and InfluxDB
    fprintf(stderr, "logger_flush: flushing influx buffer pos=%zu\n", 
            log->influx_buffer_pos);
    influx_flush(log);
    return dump(log);
}

void logger_close(logger_t *log) {
    influx_flush(log);
    if (log->curl) {
        curl_easy_cleanup(log->curl);
        curl_global_cleanup();
    }
}