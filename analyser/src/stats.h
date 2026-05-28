#ifndef STATS_H
#define STATS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/*
 * Computes per-test statistics from publisher and analyser TSV files.
 *
 * All TSV files use append mode across multiple runs. The caller must
 * supply a [start_ts, end_ts] window (Unix epoch ms) so rows from other
 * runs are excluded. Within that window, loss is calculated by joining
 * publisher and analyser on the composite key (counter, pub_timestamp).
 *
 */

#define MAX_LINE 8192
#define MAX_PATH 512
#define BURST_SECS 30   

/*
 * row_pair_t — one entry read from either TSV, keyed by (counter, pub_ts).
 * Used for both publisher and analyser so the merge-walk join is simple.
 */
typedef struct {
    long long counter;
    long long pub_ts;
    long long recv_ts;   // analyser only; 0 for publisher rows 
} row_pair_t;

/*
 * test_stats_t — all computed statistics for one test combination.
 * Fields are filled by calc_test_stats().
 */
typedef struct {
    // test identity
    int pub_qos; int sub_qos; int delay_ms; int msg_size;

    // path to output files 
    char ana_file[MAX_PATH];
    char pub_file[MAX_PATH];
    char sys_file[MAX_PATH];

    // time window for the associated test
    long long start_ts; long long end_ts;

    long long pub_attempts; // count all rows
    long long pub_success;  // count mqtt_success == 0
    double pub_success_rate; // success / attempts * 100

    long long exp_msg; // publisher successes in window
    long long actual_recv; // analyser rows in window

    long long loss_count;
    double loss_perc;

    long long out_of_order_count;
    double out_of_order_perc;

    long long dup_count;
    double dup_perc;

    double mean_rate_msgs_per_sec;

    // recv_ts differences between messages
    double mean_gap_ms;
    double stddev_gap_ms;
    long long gap_sample_count;
} test_stats_t;

typedef struct {
    int pub_qos; int sub_qos; int delay_ms; int msg_size;
    long long start_ts; long long end_ts;
} test_metadata_t;

/*
 * Main entry point. Reads publisher and analyser TSV files, filters
 * by [stats->start_ts, stats->end_ts], computes all statistics.
 *
 * stats->analyser_file, publisher_file, sys_file, start_ts, end_ts,
 * pub_qos, sub_qos, delay_ms, msg_size must be set before calling.
 *
 * Returns 0 on success, -1 on file error.
 */
int calc_test_stats(test_stats_t *stats);

/*
 * Prints relevant $SYS broker metrics from the sys TSV file for this test combination. 
 * Call after calc_test_stats().
 */
int correlate_with_sys(const test_stats_t *stats);

/* Output helpers */
void print_test_stats(const test_stats_t *stats);
void build_stats_tsv_header(void);
void build_stats_tsv_row(const test_stats_t *stats);
int read_test_metadata(const char *analyser_dir, test_metadata_t **tests_out);

#endif /* STATS_H */