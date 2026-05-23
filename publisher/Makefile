CC = gcc
TARGET = publisher

SRCS = src/main.c \
       shared/src/utils.c

CFLAGS = -Wall -Ishared/src -I/usr/include/paho-mqtt-c
LDFLAGS = -lpaho-mqtt3c

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(SRCS) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)