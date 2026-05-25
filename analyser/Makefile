CC = gcc
TARGET = analyser

SRCS = src/main.c src/logger.c shared/src/utils.c

CFLAGS = -Wall -Wextra -Isrc -Ishared/src -I/usr/include/paho-mqtt-c
LDFLAGS = -lpaho-mqtt3c

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(SRCS) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)