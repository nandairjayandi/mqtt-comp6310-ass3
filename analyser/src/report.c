#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stats.h"


int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <analyser_dir> <publisher_dir> <sys_dir>\n", argv[0]);
        return 1;
    }
    
    const char *analyser_dir = argv[1];
    const char *publisher_dir = argv[2];
    const char *sys_dir = argv[3];
    
    test_metadata_t *tests = NULL;
    int n_tests = read_test_metadata(analyser_dir, &tests); // get number of tests from metadata
    if (n_tests <= 0) {
        fprintf(stderr, "report: no test metadata found in %s/test_timestamps.tsv\n", analyser_dir);
        return 1;
    }
    fprintf(stderr, "report: found %d test runs\n", n_tests);
    
    FILE *report_fp = fopen("out/report", "w");
    if (!report_fp) {
        fprintf(stderr, "report: Failed to open out/report for writing\n");
        return 1;
    }

    build_stats_tsv_header(report_fp);

    // Process each test with its own timestamp window
    build_stats_tsv_header(report_fp);
    
    for (int i = 0; i < n_tests; i++) {
        test_stats_t stats;
        memset(&stats, 0, sizeof(stats));
        
        stats.pub_qos = tests[i].pub_qos;
        stats.sub_qos = tests[i].sub_qos;
        stats.delay_ms = tests[i].delay_ms;
        stats.msg_size = tests[i].msg_size;
        stats.start_ts = tests[i].start_ts;
        stats.end_ts = tests[i].end_ts;
        
        snprintf(stats.pub_file, sizeof(stats.pub_file),
                 "%s/pq%d_d%d_s%d.tsv",
                 publisher_dir, 
                 stats.pub_qos, 
                 stats.delay_ms, 
                 stats.msg_size
        );
        
        snprintf(stats.ana_file, sizeof(stats.ana_file),
                 "%s/pq%d_sq%d_d%d_s%d.tsv",
                 analyser_dir, 
                 stats.pub_qos, 
                 stats.sub_qos, 
                 stats.delay_ms, 
                 stats.msg_size
        );
        
        snprintf(stats.sys_file, sizeof(stats.sys_file),
                 "%s/sys_pq%d_sq%d_d%d_s%d.tsv",
                 sys_dir, 
                 stats.pub_qos, 
                 stats.sub_qos,
                 stats.delay_ms, 
                 stats.msg_size
        );
        
        FILE *check = fopen(stats.ana_file, "r");
        if (!check) {
            fprintf(stderr, "Skipping test %d: missing %s\n", i+1, stats.ana_file);
            continue;
        }
        fclose(check);
        
        if (calc_test_stats(&stats) == 0) {
            build_stats_tsv_row(report_fp, &stats);
        } else {
            fprintf(stderr, "report: failed to analyse pq=%d sq=%d d=%d s=%d\n",
                    stats.pub_qos, 
                    stats.sub_qos, 
                    stats.delay_ms, 
                    stats.msg_size
            );
        }
    }

    
    free(tests);
    return 0;
}