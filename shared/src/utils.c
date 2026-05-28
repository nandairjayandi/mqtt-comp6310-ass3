#include "utils.h"

#include <time.h>
#include <stdio.h>

long long get_timestamp_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + (long long)ts.tv_nsec / 1000000LL;
}
 
void timestamp_to_iso(long long ms, char *buf, size_t buf_size) {
    time_t secs = (time_t)(ms / 1000);
    int millis = (int)(ms % 1000);
    struct tm *t = gmtime(&secs);
 
    snprintf(
        buf, 
        buf_size,
        "%04d-%02d-%02dT%02d-%02d-%02d-%03dZ",
        t->tm_year + 1900, // unix epoch start from 1900
        t->tm_mon + 1,
        t->tm_mday,
        t->tm_hour,
        t->tm_min,
        t->tm_sec,
        millis
    );
}

void format_duration(long long ms, char *buf, size_t buf_size) {
    long long sum_secs = ms / 1000;
    int hours = (int)(sum_secs / 3600);
    int mins = (int)((sum_secs % 3600) / 60);
    int secs = (int)(sum_secs % 60);
    
    snprintf(buf, buf_size, "%02d:%02d:%02d", hours, mins, secs);
}