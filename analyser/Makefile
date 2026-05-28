CC = gcc
TARGET = analyser
REPORT_TARGET = report

SRCS = src/main.c src/logger.c src/sys_mon.c src/stats.c ../shared/src/utils.c
REPORT_SRCS = src/report.c src/stats.c src/logger.c src/sys_mon.c ../shared/src/utils.c

CFLAGS = -Wall -Wextra -Isrc -Ishared/src -I/usr/include/paho-mqtt-c
LDFLAGS = -lpaho-mqtt3c -lcurl -lm

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(SRCS) -o $(TARGET) $(LDFLAGS)

$(REPORT_TARGET): $(REPORT_SRCS)
	$(CC) $(CFLAGS) $(REPORT_SRCS) -o $(REPORT_TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET) $(REPORT_TARGET)