#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <MQTTClient.h>

#include "logger.h"
#include "../../shared/src/utils.h"

/*
 * Orchestrator that controls the publisher and records incoming messages to TSV for analysis.
 *
 * Usage: ./analyser <broker_address>
 *   e.g: ./analyser tcp://localhost:1883
 *        ./analyser tcp://your-aws-ip:1883
 *
 * 1. Connect to broker
 * 2. Subscribe to counter/# (incoming measurements) and request/go (done signal)
 * 3. Publish test params to request/qos, request/delay, request/messagesize
 * 4. Publish "start" to request/go
 * 5. Callback logs every counter/# message to TSV via logger
 * 6. On "done" from request/go; flush logger and exit
 */

// TODO currently a hardcoded parameter
#define TEST_PUB_QOS   0
#define TEST_SUB_QOS   0
#define TEST_DELAY_MS  100
#define TEST_MSG_SIZE  1
#define TEST_DURATION_S 30

#define TIMEOUT_GRACE_S 10 // How many seconds to wait beyond test duration before giving up

#define CLIENT_ID   "analyser-01"
#define MQTT_TIMEOUT 10000L

// shared state between main() and on_message() callback 
static logger_t         g_logger;
static volatile int     g_done = 0;  // set when "done" received on request/go 

/*
 * parses the publisher message format: "counter:pub_timestamp:xxx...xxx"
 * extracts counter and pub_timestamp only. the x-string is ignored.
 *
 * Returns 0 on success, -1 if the format does not match.
 */
static int parse_payload(const char *payload, long long *counter, long long *pub_ts) {
    return (sscanf(payload, "%lld:%lld:", counter, pub_ts) == 2) ? 0 : -1;
}

/*
 * extracts msg_size from topic "counter/<qos>/<delay>/<msg_size>".
 * returns msg_size on success, -1 if the topic format is unexpected.
 */
static int parse_msg_size_from_topic(const char *topic) {
    int qos, delay, msg_size;
    if (sscanf(topic, "counter/%d/%d/%d", &qos, &delay, &msg_size) == 3) {
        return msg_size;
    }
    return -1;
}

/*
 * called by paho on every received message.
 *
 * handles two topic families:
 *   counter/#  → parse payload, log to TSV
 *   request/go → watch for "done" signal from publisher
 */
static int on_message(void *context,
                      char *topic,
                      int   topic_len,
                      MQTTClient_message *msg) {
    (void)context;
    (void)topic_len;

    long long recv_ts = get_timestamp_ms();

    char payload[8192];
    int len = msg->payloadlen < (int)sizeof(payload) - 1
            ? msg->payloadlen
            : (int)sizeof(payload) - 1;

    memcpy(payload, msg->payload, len);
    payload[len] = '\0';

    if (strncmp(topic, "counter/", 8) == 0) {
        long long counter, pub_ts;
        if (parse_payload(payload, &counter, &pub_ts) == 0) {
            int msg_size = parse_msg_size_from_topic(topic);
            logger_write(&g_logger, counter, pub_ts, recv_ts, topic, msg_size);
        }

    } else if (strcmp(topic, "request/go") == 0) {
        if (strcmp(payload, "done") == 0) { g_done = 1; }
    }

    MQTTClient_freeMessage(&msg);
    MQTTClient_free(topic);
    return 1; 
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <broker_address>\n", argv[0]);
        fprintf(stderr, "  e.g: %s tcp://localhost:1883\n", argv[0]);
        return 1;
    }
    const char *broker = argv[1];

    // Init logger. TSV files written to ./logs/ 
    if (logger_init(&g_logger, "logs") != 0) {
        fprintf(stderr, "analyser: failed to init logger\n");
        return 1;
    }

    // Create and connect MQTT client 
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;

    MQTTClient_create(&client, broker, CLIENT_ID,
                      MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTClient_setCallbacks(client, NULL, NULL, on_message, NULL);

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession      = 1;

    int rc = MQTTClient_connect(client, &conn_opts);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "analyser: failed to connect to %s (rc=%d)\n",
                broker, rc);
        return 1;
    }
    printf("analyser: connected to %s\n", broker);

    // Subscribe before publishing "start" to avoid missing early messages 
    MQTTClient_subscribe(client, "counter/#",  TEST_SUB_QOS);
    MQTTClient_subscribe(client, "request/go", TEST_SUB_QOS);
    printf("analyser: subscribed to counter/# and request/go\n");

    // Publish test parameters 
    char qos_str[4], delay_str[8], size_str[8];
    snprintf(qos_str,   sizeof(qos_str),   "%d", TEST_PUB_QOS);
    snprintf(delay_str, sizeof(delay_str), "%d", TEST_DELAY_MS);
    snprintf(size_str,  sizeof(size_str),  "%d", TEST_MSG_SIZE);

    MQTTClient_publish(client, "request/qos",
                       (int)strlen(qos_str),   qos_str,   0, 0, NULL);
    MQTTClient_publish(client, "request/delay",
                       (int)strlen(delay_str), delay_str, 0, 0, NULL);
    MQTTClient_publish(client, "request/messagesize",
                       (int)strlen(size_str),  size_str,  0, 0, NULL);
    printf("analyser: published params — qos=%s delay=%sms size=%s\n",
           qos_str, delay_str, size_str);

    // pause to ensure publisher has received all request/# params before "start" fires. 200ms is generous at local/LAN latency.
    usleep(200000);

    MQTTClient_publish(client, "request/go",
                       (int)strlen("start"), "start", 0, 0, NULL);
    printf("analyser: sent 'start' — test running for %ds...\n",
           TEST_DURATION_S);

    /* Wait for "done" from publisher, with a hard timeout */
    int waited = 0;
    int limit  = TEST_DURATION_S + TIMEOUT_GRACE_S;
    while (!g_done && waited < limit) {
        sleep(1);
        waited++;
    }

    if (g_done) {
        printf("analyser: received 'done' from publisher\n");
    } else {
        printf("analyser: timeout after %ds. Flushing anyway\n", waited);
    }

    logger_flush(&g_logger);
    printf("analyser: log flushed\n");

    MQTTClient_disconnect(client, MQTT_TIMEOUT);
    MQTTClient_destroy(&client);
    printf("analyser: done\n");
    return 0;
}