#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <MQTTClient.h>

#include "logger.h"
#include "sys_mon.h"
#include "../../shared/src/utils.h"

/*
 * Orchestrator controlling publisher and records incoming messages to TSV for analysis.
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

#define TEST_DURATION_S 30
#define TIMEOUT_GRACE_S 10 // How many seconds to wait beyond test duration before giving up
#define PARAM_SETTLE_MS 500 // How many ms to wait after publishing params before "start"
#define INTER_TEST_SLEEP_S 2 // Seconds pause between tests to let broker settle

#define CLIENT_ID "analyser-01"
#define MQTT_TIMEOUT 10000L

// shared state between main() and on_message() callback 
static logger_t g_logger;
static sys_mon_t g_sys;
static volatile int g_done = 0; // set when "done" received on request/go msg
static MQTTClient g_client;
static const char *g_broker;

// Testing parameters
static const int SUB_QOS_VALS[] = {0, 1, 2};
static const int PUB_QOS_VALS[] = {0, 1, 2};
static const int DELAY_VALS[] = {0, 100};
static const int MSG_SIZE_VALS[] = {1, 1000, 4000};

#define N_SUB_QOS 3
#define N_PUB_QOS 3
#define N_DELAYS 2
#define N_MSG_SIZES 3
#define N_TESTS (N_SUB_QOS * N_PUB_QOS * N_DELAYS * N_MSG_SIZES)  // equal 54

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
static int on_message(void *context, char *topic, int topic_len, MQTTClient_message *msg) {
    (void)context; (void)topic_len;

    long long recv_ts = get_timestamp_ms();

    char payload[8192];
    int len = msg->payloadlen < (int)sizeof(payload) - 1
            ? msg->payloadlen : (int)sizeof(payload) - 1;

    memcpy(payload, msg->payload, len);
    payload[len] = '\0';

    if (strncmp(topic, "counter/", 8) == 0) {
        char expected[64];
        snprintf(expected, sizeof(expected), 
            "counter/%d/%d/%d",
            g_logger.pub_qos, g_logger.delay_ms, g_logger.msg_size
        );

        if (strcmp(topic, expected) != 0) {
            MQTTClient_freeMessage(&msg);
            MQTTClient_free(topic);
            return 1;  /* discard — stale message from previous test */
        }

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

/*
 * (Re)connects the analyser MQTT client and subscribes to counter/# and request/go at the given sub_qos
 */
static int connect_and_subscribe(int sub_qos) {
    MQTTClient_connectOptions conn = MQTTClient_connectOptions_initializer;
    conn.keepAliveInterval = 20; conn.cleansession = 1;
 
    int rc = MQTTClient_connect(g_client, &conn);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "analyser: connect failed (rc=%d)\n", rc);
        return -1;
    }
 
    MQTTClient_subscribe(g_client, "counter/#", sub_qos);
    MQTTClient_subscribe(g_client, "request/go", sub_qos);
    printf("analyser: connected, (re)subscribed at sub_qos=%d\n", sub_qos);
    return 0;
}

static void run_test(int test_num, int pub_qos, int sub_qos, int delay_ms, int msg_size) {
    printf("\n[%d/%d] pq=%d sq=%d delay=%dms size=%d\n",
        test_num, N_TESTS, pub_qos, sub_qos, delay_ms, msg_size
    );
 
    g_done = 0;
 
    // Update logger and sys_monitor with test metadata
    logger_set_test(&g_logger, pub_qos, sub_qos, delay_ms, msg_size);
    sys_mon_start(&g_sys, pub_qos, sub_qos, delay_ms, msg_size);
 
    // Publish params to publisher
    char qos_str[4], delay_str[8], size_str[8];
    snprintf(qos_str, sizeof(qos_str), "%d", pub_qos);
    snprintf(delay_str, sizeof(delay_str), "%d", delay_ms);
    snprintf(size_str, sizeof(size_str), "%d", msg_size);
 
    MQTTClient_publish(g_client, "request/qos",
                       (int)strlen(qos_str), qos_str, 1, 0, NULL);
    MQTTClient_publish(g_client, "request/delay",
                       (int)strlen(delay_str), delay_str, 1, 0, NULL);
    MQTTClient_publish(g_client, "request/messagesize",
                       (int)strlen(size_str), size_str, 1, 0, NULL);
 
    // Wait for publisher to receive all params before sending "start". On high-latency internet paths likely this should be increased
    // prevents race condition 
    usleep(PARAM_SETTLE_MS * 1000);
 
    MQTTClient_publish(g_client, "request/go", (int)strlen("start"), "start", 1, 0, NULL);
    printf("analyser: 'start' sent. Running %ds..\n", TEST_DURATION_S);
 
    // Wait for "done" with hard timeout
    int waited = 0;
    int limit  = TEST_DURATION_S + TIMEOUT_GRACE_S;
    while (!g_done && waited < limit) {
        sleep(1);
        waited++;
    }
 
    if (g_done) {
        printf("analyser: 'done' received after %ds\n", waited);
    } else {
        printf("analyser: timeout after %ds. Flushing anyway\n", waited);
    }
 
    logger_flush(&g_logger);
    sys_mon_stop(&g_sys);
}


int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <broker_address>\n", argv[0]);
        fprintf(stderr, "e.g: %s tcp://localhost:1883\n", argv[0]);
        return 1;
    }
    g_broker = argv[1];

    // Init logger. TSV files written to ./logs/ 
    if (logger_init(&g_logger, "logs") != 0) {
        fprintf(stderr, "analyser: failed to init logger\n");
        return 1;
    }

    if (sys_mon_init(&g_sys, g_broker, "logs/sys") != 0) { 
        fprintf(stderr, "analyser: failed to init sys_mon\n");
        return 1; 
    }

    // Create and connect MQTT client 
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_create(&g_client, g_broker, CLIENT_ID, MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTClient_setCallbacks(g_client, NULL, NULL, on_message, NULL);

    printf("analyser: starting %d-test suite against %s\n", N_TESTS, g_broker);
 
    int test_num = 1;
    int first_connection = 1;
 
    for (int si = 0; si < N_SUB_QOS; si++) {
        int sub_qos = SUB_QOS_VALS[si];
 
        if (!first_connection) {
            MQTTClient_disconnect(g_client, MQTT_TIMEOUT);
            printf("analyser: disconnected — reconnecting at sub_qos=%d\n",
                   sub_qos);
        }
        first_connection = 0;
 
        if (connect_and_subscribe(sub_qos) != 0) return 1;
 
        for (int pi = 0; pi < N_PUB_QOS; pi++) {
            for (int di = 0; di < N_DELAYS; di++) {
                for (int mi = 0; mi < N_MSG_SIZES; mi++) {
 
                    run_test(test_num++,
                             PUB_QOS_VALS[pi], sub_qos,
                             DELAY_VALS[di], MSG_SIZE_VALS[mi]);
 
                    // brief pause between tests to let broker settle down
                    if (test_num <= N_TESTS) sleep(INTER_TEST_SLEEP_S);
                }
            }
        }
    }
 
    MQTTClient_disconnect(g_client, MQTT_TIMEOUT);
    MQTTClient_destroy(&g_client);
    sys_mon_destroy(&g_sys);
 
    printf("\nanalyser: all %d tests complete. Results in logs/\n", N_TESTS);
    return 0;
}