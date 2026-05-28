#include "stats.h"

/*
 * Design notes:
 *
 *   All TSV files are written in append mode so one file accumulates
 *   data across multiple test runs. The [start_ts, end_ts] window
 *   passed in test_stats_t filters rows to a single run.
 *
 *   Loss is calculated by joining publisher and analyser on the
 *   composite key (counter, pub_timestamp). Using only counter would
 *   produce false matches across runs since counters reset to 0 each
 *   burst.
 *
 *   Out-of-order is measured on arrival order (file row order) before
 *   any sorting, which is the only meaningful definition — it reflects
 *   what the analyser actually observed.
 *
 *   Gap is measured using recv_timestamp differences between rows
 *   whose counter values are strictly consecutive within the same run,
 *   identified by the pub_ts proximity check.
 */


// simple counter comparator for quick sort and walking loss analysis
static int cmp_row_pair(const void *a, const void *b) {
    const row_pair_t *ra = (const row_pair_t *)a;
    const row_pair_t *rb = (const row_pair_t *)b;
    if (ra->counter != rb->counter) {
        return (ra->counter > rb->counter) - (ra->counter < rb->counter);
    }
    return (ra->pub_ts > rb->pub_ts) - (ra->pub_ts < rb->pub_ts);
}

// increase memory size for array
static row_pair_t *grow(row_pair_t *arr, size_t *cap) {
    *cap *= 2;
    row_pair_t *tmp = realloc(arr, *cap * sizeof(row_pair_t));
    if (!tmp) {
        fprintf(stderr, "stats: realloc failed\n");
        free(arr);
    }
    return tmp;
}

/*
 * Reads publisher TSV, keeps only rows where: pub_timestamp is within [start_ts, end_ts] and mqtt_success == MQTTCLIENT_SUCCESS (0)
 *
 * Also counts all attempts (any mqtt_success) within the window for the publisher_success_rate calculation.
 *
 * Returns sorted array of (counter, pub_ts) pairs, caller must free.
 */
static row_pair_t *read_publisher(const char *path,
                                  long long start_ts, long long end_ts,
                                  long long *out_count,
                                  long long *attempts, // how many attempts to publish a message regardless whether it was a success or not
                                  long long *successes) {
    *out_count = 0; *attempts  = 0; *successes = 0;

    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "stats: cannot open publisher file: %s\n", path);
        return NULL;
    }

    size_t cap = 10000, n = 0;
    row_pair_t *rows = malloc(cap * sizeof(row_pair_t));
    if (!rows) { fclose(fp); return NULL; }

    char line[MAX_LINE];
    int is_header = 1;

    while (fgets(line, sizeof(line), fp)) {
        if (is_header) { is_header = 0; continue; }

        long long counter, pub_ts;
        char topic[256];
        int msg_size, mqtt_success;

        if (sscanf(line, "%lld\t%lld\t%255[^\t]\t%d\t%d",
                   &counter, &pub_ts, topic, &msg_size, &mqtt_success) != 5)
            continue;

        // filters counter to selected timestamp window and add to attempts counter
        if (pub_ts < start_ts || pub_ts > end_ts) continue;
        (*attempts)++;

        // omit failed publish and add to succesful counter
        if (mqtt_success != 0) continue; 
        (*successes)++;

        if (n >= cap) {
            rows = grow(rows, &cap);
            if (!rows) { fclose(fp); return NULL; }
        }

        rows[n].counter = counter;
        rows[n].pub_ts = pub_ts;
        rows[n].recv_ts = 0;
        n++;
    }
    fclose(fp);

    qsort(rows, n, sizeof(row_pair_t), cmp_row_pair);
    *out_count = (long long)n;
    return rows;
}

/*
 * Reads analyser TSV, keeps only rows where pub_timestamp is within
 * [start_ts, end_ts].
 *
 * Computes out-of-order and duplicate counts on arrival order (file
 * row order) before sorting. Returns sorted array of (counter, pub_ts,
 * recv_ts) triples, caller must free.
 *
 * out-of-order: counter decreased compared to previous row
 * duplicate: (counter, pub_ts) identical to previous row
 */
static row_pair_t *read_analyser(const char *path,
                                 long long start_ts, long long end_ts,
                                 long long *out_count,
                                 long long *ooo_count, 
                                 long long *dup_count) {
    *out_count = 0; *ooo_count = 0; *dup_count = 0;

    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "stats: cannot open analyser file: %s\n", path);
        return NULL;
    }

    size_t cap = 10000, n = 0;
    row_pair_t *rows = malloc(cap * sizeof(row_pair_t));
    if (!rows) { fclose(fp); return NULL; }

    char line[MAX_LINE];
    int is_header = 1;
    long long prev_counter = -1;
    long long prev_pub_ts  = -1;

    while (fgets(line, sizeof(line), fp)) {
        if (is_header) { is_header = 0; continue; }

        long long counter, pub_ts, recv_ts, latency;
        char topic[256];
        int msg_size, pub_qos, sub_qos, delay_ms;

        if (sscanf(line,
                   "%lld\t%lld\t%255[^\t]\t%d\t%lld\t%lld\t%d\t%d\t%d",
                   &counter, &pub_ts, topic, &msg_size,
                   &recv_ts, &latency, &pub_qos, &sub_qos, &delay_ms) != 9)
            continue;

        // Filters run window window using pub_ts
        if (pub_ts < start_ts || pub_ts > end_ts) continue;

        /*
         * Out-of-order: counter went backwards in arrival order.
         */
        if (prev_counter != -1 && counter < prev_counter) (*ooo_count)++;

        /*
         * Duplicate: exact (counter, pub_ts) match to any previous row.
         */
        if (prev_counter != -1 && counter == prev_counter && pub_ts  == prev_pub_ts) (*dup_count)++;

        if (n >= cap) {
            rows = grow(rows, &cap);
            if (!rows) { fclose(fp); return NULL; }
        }

        rows[n].counter = counter;
        rows[n].pub_ts = pub_ts;
        rows[n].recv_ts = recv_ts;
        n++;

        prev_counter = counter;
        prev_pub_ts = pub_ts;
    }
    fclose(fp);

    qsort(rows, n, sizeof(row_pair_t), cmp_row_pair);
    *out_count = (long long)n;
    return rows;
}

/*
 * Compares publisher and analyser arrays pre sorted based on (counter, pub_ts).
 * Publisher rows with no matching analyser row are counted as lost.
 */
static void calc_loss(const row_pair_t *pub, long long pub_n,
                      const row_pair_t *ana, long long ana_n,
                      long long *loss_count) {
    *loss_count = 0;
    long long pi = 0, ai = 0; // publisher index and analyser index respectively

    while (pi < pub_n && ai < ana_n) {
        int cmp = cmp_row_pair(&pub[pi], &ana[ai]);
        if (cmp == 0) {
            pi++; ai++; // match
        } else if (cmp < 0) {
            (*loss_count)++; // publisher sent analyser never received 
            pi++;
        } else {
            ai++; // analyser received but publisher missing, expect zero
        }
    }
    *loss_count += (pub_n - pi); // remaining publisher rows = lost 
}

/*
 * Iterates sorted analyser rows. For each pair of adjacent rows where
 * counter values are strictly consecutive AND pub_ts difference is
 * within one burst window (≤ 30s), computes the recv_ts gap.
 *
 * The pub_ts proximity check prevents measuring gaps across run
 * boundaries where consecutive counter values coincidentally appear
 * from different runs.
 */
static void calc_gaps(const row_pair_t *rows, long long n, double *mean_gap, double *stddev_gap, long long *sample_count) {
    *mean_gap = 0.0; *stddev_gap = 0.0; *sample_count = 0;
    long long gap_sum = 0; long long gap_sum_sq = 0; long long samples = 0;

    if (n < 2) return;

    for (long long i = 0; i < n - 1; i++) {
        if (rows[i+1].counter != rows[i].counter + 1) continue;

        /*
         * Verify same run: pub_ts difference should be small.
         * 30000ms = 30s burst. Cross-run pairs would differ by minutes.
         */
        long long pub_diff = rows[i+1].pub_ts - rows[i].pub_ts;
        if (pub_diff < 0 || pub_diff > (BURST_SECS * 1000LL)) continue;

        long long gap = rows[i+1].recv_ts - rows[i].recv_ts;
        if (gap < 0) continue; // negative gaps are measurement artefacts

        gap_sum += gap;
        gap_sum_sq += gap * gap;
        samples++;
    }

    if (samples == 0) return;

    *mean_gap = (double)gap_sum / samples;
    double var = ((double)gap_sum_sq / samples) - (*mean_gap * *mean_gap);
    *stddev_gap = sqrt(var > 0.0 ? var : 0.0);
    *sample_count = samples;
}

int calc_test_stats(test_stats_t *stats) {
    if (!stats) return -1;

    stats->pub_attempts = 0;
    stats->pub_success = 0;
    stats->pub_success_rate = 0.0;
    stats->exp_msg = 0;
    stats->actual_recv = 0;
    stats->loss_count = 0;
    stats->loss_perc = 0.0;
    stats->out_of_order_count = 0;
    stats->out_of_order_perc = 0.0;
    stats->dup_count = 0;
    stats->dup_perc = 0.0;
    stats->mean_rate_msgs_per_sec = 0.0;
    stats->mean_gap_ms = 0.0;
    stats->stddev_gap_ms = 0.0;
    stats->gap_sample_count = 0;

    long long pub_n = 0;
    row_pair_t *pub = read_publisher(
            stats->pub_file, 
            stats->start_ts, stats->end_ts,
            &pub_n,
            &stats->pub_attempts,
            &stats->pub_success
    );
    if (!pub && stats->pub_attempts == 0) {
        fprintf(stderr, "stats: no publisher data in window for %s\n", stats->pub_file);
        return -1;
    }

    if (stats->pub_attempts > 0)
        stats->pub_success_rate = (double)stats->pub_success_rate / stats->pub_attempts * 100.0;

    stats->exp_msg = pub_n;

    long long ana_n = 0, ooo = 0, dup = 0;
    row_pair_t *ana = read_analyser(
        stats->ana_file,
        stats->start_ts, stats->end_ts,
        &ana_n, &ooo, &dup
    );

    if (!ana && ana_n == 0) {
        fprintf(stderr, "stats: no analyser data in window for %s\n",
                stats->ana_file);
        free(pub);
        return -1;
    }

    stats->actual_recv = ana_n;
    stats->out_of_order_count = ooo;
    stats->dup_count = dup;

    if (ana_n > 0) {
        stats->out_of_order_perc = (double)ooo / ana_n * 100.0;
        stats->dup_perc = (double)dup / ana_n * 100.0;
    }

    /* Loss: publisher rows not present in analyser */
    if (pub && ana) {
        calc_loss(pub, pub_n, ana, ana_n, &stats->loss_count);
        if (pub_n > 0) {
            stats->loss_perc = (double)stats->loss_count / pub_n * 100.0;
        }
    }

    /* Inter-message gap */
    if (ana) {
        calc_gaps(ana, ana_n, &stats->mean_gap_ms, &stats->stddev_gap_ms, &stats->gap_sample_count);
    }

    /*
     * Mean rate: use actual recv window if available, else assume 30s.
     * recv window = last recv_ts - first recv_ts in sorted ana array.
     */
    if (ana_n >= 2) {
        long long duration_ms = ana[ana_n - 1].recv_ts - ana[0].recv_ts;
        if (duration_ms > 0)
            stats->mean_rate_msgs_per_sec =
                (double)ana_n / (duration_ms / 1000.0);
        else
            stats->mean_rate_msgs_per_sec = (double)ana_n / BURST_SECS;
    } else if (ana_n == 1) {
        stats->mean_rate_msgs_per_sec = 1.0 / BURST_SECS;
    }

    free(pub);
    free(ana);
    return 0;
}

/*
 * Correlate report with SYS messages
 */
int correlate_with_sys(const test_stats_t *stats) {
    FILE *fp = fopen(stats->sys_file, "r");
    if (!fp) {
        fprintf(stderr, "stats: cannot open sys file: %s\n", stats->sys_file);
        return -1;
    }

    printf("[SYS] correlate_with_sys: pq=%d sq=%d delay=%d size=%d "
           "loss=%.4f%% ooo=%.4f%% dup=%.4f%%\n",
           stats->pub_qos, stats->sub_qos,
           stats->delay_ms, stats->msg_size,
           stats->loss_perc,
           stats->out_of_order_perc,
           stats->dup_perc
    );

    char line[MAX_LINE];
    int is_header = 1;

    while (fgets(line, sizeof(line), fp)) {
        if (is_header) { is_header = 0; continue; }

        long long recv_ts;
        char topic[256], value[256];

        if (sscanf(line, "%lld\t%255[^\t]\t%255[^\n]",
                   &recv_ts, topic, value) != 3) continue;

        if (recv_ts < stats->start_ts || recv_ts > stats->end_ts) continue;

        if (strstr(topic, "publish/messages/dropped"))
            printf("[SYS] dropped msgs: %s\n", value);
        else if (strstr(topic, "heap/current"))
            printf("[SYS] heap bytes: %s\n", value);
        else if (strstr(topic, "load/messages/received/1min"))
            printf("[SYS] load recv 1min: %s\n", value);
        else if (strstr(topic, "load/publish/dropped/1min"))
            printf("[SYS] load dropped 1min: %s\n", value);
        else if (strstr(topic, "store/messages/bytes"))
            printf("[SYS] store queue bytes: %s\n", value);
        else if (strstr(topic, "messages/stored"))
            printf("[SYS] messages stored: %s\n", value);
    }

    fclose(fp);
    return 0;
}

int read_test_metadata(const char *analyser_dir, test_metadata_t **tests_out) {
    char metadata_path[MAX_PATH + 32];
    snprintf(metadata_path, sizeof(metadata_path), "%s/test_timestamps.tsv", analyser_dir);
    
    FILE *fp = fopen(metadata_path, "r");
    if (!fp) {
        fprintf(stderr, "stats: cannot open metadata file: %s\n", metadata_path);
        return -1;
    }
    
    test_metadata_t *tests = NULL;
    int count = 0;
    int capacity = 100;  // Starting capacity
    
    tests = malloc(capacity * sizeof(test_metadata_t));
    if (!tests) {
        fclose(fp);
        return -1;
    }
    
    char line[MAX_LINE];
    int is_header = 1;
    
    while (fgets(line, sizeof(line), fp)) {
        if (is_header) { is_header = 0; continue; }
        
        if (count >= capacity) {
            capacity *= 2;
            test_metadata_t *tmp = realloc(tests, capacity * sizeof(test_metadata_t));
            if (!tmp) {
                free(tests);
                fclose(fp);
                return -1;
            }
            tests = tmp;
        }
        
        if (sscanf(line, "%d\t%d\t%d\t%d\t%lld\t%lld",
                   &tests[count].pub_qos,
                   &tests[count].sub_qos,
                   &tests[count].delay_ms,
                   &tests[count].msg_size,
                   &tests[count].start_ts,
                   &tests[count].end_ts) == 6) {
            count++;
        }
    }
    
    fclose(fp);
    *tests_out = tests;
    return count;
}

void print_test_stats(const test_stats_t *stats) {
    printf("[REPORT] Test: pq=%d sq=%d delay=%dms size=%d\n",
        stats->pub_qos, stats->sub_qos, stats->delay_ms, stats->msg_size
    );
    printf("[REPORT] Publisher attempts: %lld\n", stats->pub_attempts);
    printf("[REPORT] Publisher successes: %lld (%.2f%%)\n", stats->pub_success, stats->pub_success_rate);
    printf("[REPORT] Expected (sent): %lld\n", stats->exp_msg);
    printf("[REPORT] Received: %lld\n", stats->actual_recv);
    printf("[REPORT] Lost: %lld (%.4f%%)\n", stats->loss_count, stats->loss_perc);
    printf("[REPORT] Out of order: %lld (%.4f%%)\n", stats->out_of_order_count, stats->out_of_order_perc);
    printf("[REPORT] Duplicates: %lld (%.4f%%)\n", stats->dup_count, stats->dup_perc);
    printf("[REPORT] Mean rate: %.2fmsg/s\n", stats->mean_rate_msgs_per_sec);
    printf("[REPORT] Mean gap (between message): %.3fms (n=%lld)\n", stats->mean_gap_ms, stats->gap_sample_count);
    printf("[REPORT] Stddev gap: %.3fms\n", stats->stddev_gap_ms);
}

void build_stats_tsv_header(void) {
    printf("pub_qos\tsub_qos\tdelay_ms\tmsg_size\t"
           "pub_attempts\tpub_successes\tpub_success_rate\t"
           "expected\treceived\tlost\tloss_pct\t"
           "out_of_order\tooo_pct\t"
           "duplicates\tdup_pct\t"
           "mean_rate_msg_per_s\t"
           "mean_gap_ms\tstddev_gap_ms\tgap_samples\n"
    );
}

void build_stats_tsv_row(const test_stats_t *stats) {
    printf("%d\t%d\t%d\t%d\t"
           "%lld\t%lld\t%.2f\t"
           "%lld\t%lld\t"
           "%lld\t%.4f\t"
           "%lld\t%.4f\t"
           "%lld\t%.4f\t"
           "%.2f\t"
           "%.3f\t%.3f\t%lld\n",
           stats->pub_qos, stats->sub_qos, stats->delay_ms, stats->msg_size,
           stats->pub_attempts, stats->pub_success, stats->pub_success_rate,
           stats->exp_msg, stats->actual_recv,
           stats->loss_count, stats->loss_perc,
           stats->out_of_order_count, stats->out_of_order_perc,
           stats->dup_count, stats->dup_perc,
           stats->mean_rate_msgs_per_sec,
           stats->mean_gap_ms, stats->stddev_gap_ms, stats->gap_sample_count
    );
}
