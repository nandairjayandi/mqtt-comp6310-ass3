#include "logger.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

extern long long get_timestamp_ms(void);
extern void timestamp_to_iso(long long ms, char *buf, size_t buf_size);

static void build_filepath(const logger_t *log, char *path, size_t path_size) {
    char iso[32];
    timestamp_to_iso(get_timestamp_ms(), iso, sizeof(iso));

    snprintf(path, path_size,
             "%s/%lld_%s.tsv",
             log->output_dir,
             log->start_counter,
             iso
    );
}

static int dump(logger_t *log) {
    if (log->count == 0) { return 0; }

    char path[LOGGER_DIR_MAX + 64];
    build_filepath(log, path, sizeof(path));

    FILE *fp = fopen(path, "w");
    if (fp == NULL) {
        fprintf(stderr, "logger: failed to open '%s': %s\n", path, strerror(errno));
        return -1;
    }

    fprintf(fp, "counter\tpub_timestamp\trecv_timestamp\ttopic\tmsg_size\n");

    int write_error = 0;
    for (int i = 0; i < log->count; i++) {
        const log_entry_t *e = &log->buf[i];

        int ret = fprintf(fp, "%lld\t%lld\t%lld\t%s\t%d\n",
                          e->counter,
                          e->pub_timestamp,
                          e->recv_timestamp,
                          e->topic,
                          e->msg_size
        );

        if (ret < 0) {
            fprintf(stderr, "logger: write error at entry %d\n", i);
            write_error = 1;
            break;
        }
    }

    fclose(fp);

    if (write_error) {
        return -1;
    }

    // resets buffer i.e. next write starts fresh
    log->count = 0;
    log->start_counter = -1;  /* -1 = unset, updated on next logger_write() */

    return 0;
}

int logger_init(logger_t *log, const char *output_dir) {
    memset(log, 0, sizeof(logger_t));

    strncpy(log->output_dir, output_dir, LOGGER_DIR_MAX - 1);
    log->output_dir[LOGGER_DIR_MAX - 1] = '\0';

    log->count         = 0;
    log->start_counter = -1;

    if (mkdir(output_dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "logger: failed to create directory '%s': %s\n", output_dir, strerror(errno));
        return -1;
    }

    return 0;
}

int logger_write(logger_t *log, long long counter, long long pub_timestamp, long long recv_timestamp, const char *topic, int msg_size) {
    if (log->count == 0) { log->start_counter = counter; }

    log_entry_t *e = &log->buf[log->count];

    e->counter = counter;
    e->pub_timestamp = pub_timestamp;
    e->recv_timestamp = recv_timestamp;
    strncpy(e->topic, topic, LOGGER_TOPIC_MAX - 1);
    e->topic[LOGGER_TOPIC_MAX - 1] = '\0';
    e->msg_size = msg_size;

    log->count++;

    // when the log count reaches certain size (2048) dump the buffer
    if (log->count >= LOGGER_BUF_SIZE) {
        return dump(log);
    }

    return 0;
}

int logger_flush(logger_t *log) {
    return dump(log);
}