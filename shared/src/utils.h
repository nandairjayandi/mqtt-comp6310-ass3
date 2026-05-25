#ifndef UTILS_H
#define UTILS_H
 
#include <stddef.h>

long long get_timestamp_ms(void);

void timestamp_to_iso(long long ms, char *buf, size_t buf_size);

#endif