#include "logger.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>

static unsigned long long g_message_counter = 0;

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

static void build_pending_path(const logger_t *log, char *path, size_t path_size) {
    snprintf(path, path_size,
             "%s/pq%d_sq%d_d%d_s%d.pending",
             log->output_dir,
             log->pub_qos, log->sub_qos,
             log->delay_ms, log->msg_size);
}

static int dump_to_pending(logger_t *log) {
    if (log->count == 0) return 0;

    if (!log->pending_fp) {
        fprintf(stderr, "logger: dump_to_pending called but pending_fp is NULL\n");
        log->count = 0;
        return -1;
    }

    int write_error = 0;
    for (int i = 0; i < log->count; i++) {
        const log_entry_t *e = &log->buf[i];
        int ret = fprintf(log->pending_fp,
                          "%lld\t%lld\t%s\t%d\t%lld\t%d\t%d\t%d\t%d\n",
                          e->counter,
                          e->pub_ts,
                          e->topic,
                          e->msg_size,
                          e->recv_ts,
                          e->latency_ms,
                          log->pub_qos,
                          log->sub_qos,
                          log->delay_ms);
        if (ret < 0) {
            fprintf(stderr, "logger: write error at entry %d\n", i);
            write_error = 1;
            break;
        }
    }

    log->count = 0;
    return write_error ? -1 : 0;
}

static int influx_send_point(logger_t *log, long long pub_timestamp, int latency_ms) {
    if (!log->curl) return 0;  // InfluxDB not initialized

    char line[256];
    snprintf(line, sizeof(line),
        "mqtt_latency,"
        "pub_qos=%d,sub_qos=%d,delay_ms=%d,msg_size=%d "
        "latency_ms=%di,msg_count=1i "
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
    // Flush any leftover data from the previous test before switching
    if (log->count > 0) dump_to_pending(log);

    // Open a fresh pending file for this test
    build_pending_path(log, log->pending_path, sizeof(log->pending_path));
    log->pending_fp = fopen(log->pending_path, "w");
    if (!log->pending_fp) {
        fprintf(stderr, "logger: failed to open pending file '%s': %s\n",
                log->pending_path, strerror(errno));
    }

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
    log->influx_sample_rate = DEFAULT_SAMPLE_RATE;
    log->pending_fp = NULL;
    log->pending_path[0] = '\0';

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

    log->influx_sample_rate = DEFAULT_SAMPLE_RATE;
    const char *sample_rate_str = getenv("INFLUXDB_SAMPLE_RATE");
    if (sample_rate_str) {
        int rate = atoi(sample_rate_str);
        if (rate > 0) {
            log->influx_sample_rate = rate;
            fprintf(stderr, "logger: Using custom sample rate: 1/%d\n", log->influx_sample_rate);
        }
    }

    // Add timeouts to prevent hanging
    curl_easy_setopt(log->curl, CURLOPT_TIMEOUT_MS, 100L);  // 100ms total timeout
    curl_easy_setopt(log->curl, CURLOPT_CONNECTTIMEOUT_MS, 50L);  // 50ms connect
    curl_easy_setopt(log->curl, CURLOPT_NOSIGNAL, 1L);  // Don't use signals

    strncpy(log->influx_url, url, sizeof(log->influx_url)-1);
    strncpy(log->influx_token, token, sizeof(log->influx_token)-1);
    strncpy(log->influx_org, org, sizeof(log->influx_org)-1);
    strncpy(log->influx_bucket, bucket, sizeof(log->influx_bucket)-1);
    log->influx_buffer_pos = 0;

    fprintf(stderr, "logger: InfluxDB initialized at %s (sampling rate: 1/%d)\n", url, log->influx_sample_rate);
    return 0;
}

int logger_write(logger_t *log, long long counter, long long pub_ts, long long recv_ts, const char *topic, int msg_size) {
    log_entry_t *e = &log->buf[log->count];

    e->counter = counter;
    e->pub_ts = pub_ts;
    strncpy(e->topic, topic, LOGGER_TOPIC_MAX - 1);
    e->topic[LOGGER_TOPIC_MAX - 1] = '\0';
    e->msg_size = msg_size;

    e->recv_ts = recv_ts;
    e->latency_ms = (int)(recv_ts - pub_ts);

    log->count++;

    if (log->curl && (++g_message_counter % log->influx_sample_rate == 0)) {
        influx_send_point(log, pub_ts, e->latency_ms);
    }

    // Flush TSV buffer when full
    if (log->count >= LOGGER_BUF_SIZE) {
        return dump_to_pending(log);
    }
    return 0;
}

int write_ts_meta(const logger_t *log, long long start_ts, long long end_ts) {
    char metadata_path[LOGGER_DIR_MAX + 32];
    snprintf(metadata_path, sizeof(metadata_path), "%s/test_timestamps.tsv", log->output_dir);

    FILE *fp = fopen(metadata_path, "a");
    if (fp == NULL) {
        fprintf(stderr, "logger: failed to open metadata file '%s': %s\n",
                metadata_path, strerror(errno));
        return -1;
    }

    // Check if file is empty to write header
    fseek(fp, 0, SEEK_END);
    if (ftell(fp) == 0) {
        fprintf(fp, "pub_qos\tsub_qos\tdelay_ms\tmsg_size\tstart_ts\tend_ts\tretries\n");
    }

    fprintf(fp, "%d\t%d\t%d\t%d\t%lld\t%lld\t%d\n",
            log->pub_qos, log->sub_qos, log->delay_ms, log->msg_size,
            start_ts, end_ts,
            log->test_success);

    fclose(fp);
    return 0;
}

int logger_flush(logger_t *log) {
    // Flush both TSV and InfluxDB
    fprintf(stderr, "logger_flush: flushing influx buffer pos=%zu\n",
            log->influx_buffer_pos);

    /* Send any remaining entries to InfluxDB */
    if (log->curl) {
        for (int i = 0; i < log->count; i++) {
            if (++g_message_counter % log->influx_sample_rate == 0) {
                log_entry_t *e = &log->buf[i];
                influx_send_point(log, e->pub_ts, e->latency_ms);
            }
        }
    }
    influx_flush(log);

    /* Flush in-memory buffer to pending file */
    return dump_to_pending(log);
}

int logger_commit(logger_t *log) {
    /*
     * 1. Flush any remaining in-memory entries to the pending file.
     * 2. Close the pending file.
     * 3. Open (or create) the real combo TSV in append mode.
     * 4. Write the TSV header if the file is empty.
     * 5. Copy all rows from the pending file into the real file.
     * 6. Delete the pending file.
     */
    if (!log->pending_fp && log->pending_path[0] == '\0') {
        return 0; /* nothing to commit */
    }

    /* Step 1: flush buffer */
    if (dump_to_pending(log) != 0) {
        fprintf(stderr, "logger_commit: dump_to_pending failed\n");
    }

    /* Step 2: close pending file */
    if (log->pending_fp) {
        fclose(log->pending_fp);
        log->pending_fp = NULL;
    }

    /* Step 3: open real TSV */
    char real_path[LOGGER_DIR_MAX + 80];
    build_filepath(log, real_path, sizeof(real_path));

    FILE *out = fopen(real_path, "a");
    if (!out) {
        fprintf(stderr, "logger_commit: failed to open '%s': %s\n",
                real_path, strerror(errno));
        remove(log->pending_path);
        log->pending_path[0] = '\0';
        return -1;
    }

    /* Step 4: write header if the real file was just created (empty) */
    fseek(out, 0, SEEK_END);
    if (ftell(out) == 0) {
        fprintf(out,
                "counter\tpub_timestamp\ttopic\tmsg_size\t"
                "recv_timestamp\tlatency_ms\tpub_qos\tsub_qos\tdelay_ms\n");
    }

    /* Step 5: copy pending rows into real file */
    FILE *in = fopen(log->pending_path, "r");
    if (!in) {
        fprintf(stderr, "logger_commit: cannot re-open pending file '%s': %s\n",
                log->pending_path, strerror(errno));
        fclose(out);
        remove(log->pending_path);
        log->pending_path[0] = '\0';
        return -1;
    }

    char copy_buf[65536];
    size_t n;
    int copy_error = 0;
    while ((n = fread(copy_buf, 1, sizeof(copy_buf), in)) > 0) {
        if (fwrite(copy_buf, 1, n, out) != n) {
            fprintf(stderr, "logger_commit: write error copying pending data\n");
            copy_error = 1;
            break;
        }
    }

    fclose(in);
    fclose(out);

    /* Step 6: delete pending file */
    remove(log->pending_path);
    log->pending_path[0] = '\0';

    return copy_error ? -1 : 0;
}

int logger_discard(logger_t *log) {
    /*
     * Discard all pending data for the current (failed) test:
     *   - close and delete the pending file
     *   - drop the InfluxDB buffer (do not send metrics for a failed run)
     *   - reset in-memory buffer count
     */
    if (log->pending_fp) {
        fclose(log->pending_fp);
        log->pending_fp = NULL;
    }

    if (log->pending_path[0] != '\0') {
        remove(log->pending_path);
        log->pending_path[0] = '\0';
    }

    /* Drop any unsent InfluxDB data for this failed run */
    log->influx_buffer_pos = 0;

    log->count = 0;
    return 0;
}

void logger_close(logger_t *log) {
    influx_flush(log);
    if (log->curl) {
        curl_easy_cleanup(log->curl);
        curl_global_cleanup();
    }
    /* Safety: discard any uncommitted pending file. */
    if (log->pending_fp) {
        fclose(log->pending_fp);
        log->pending_fp = NULL;
        remove(log->pending_path);
    }
}