#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <errno.h>

#include <MQTTClient.h>

#include "../../shared/src/utils.h"

/*
 * Publisher code that listens for instructions from the analyser and publishes bursts of messages to the broker. 
 * Writes a TSV of every sent message as the ground truth for loss analysis.
 *
 * Usage: ./publisher <broker_address>
 *   e.g: ./publisher tcp://localhost:1883    (local / docker)
 *        ./publisher tcp://your-aws-ip:1883  (remote)
 *
 *   1. Connect to broker
 *   2. Subscribe to request/#
 *   3. Wait for request/qos, request/delay, request/messagesize to arrive
 *   4. On request/go = "start": run 30s burst, writing each sent
 *      message to TSV
 *   5. On burst end: publish "done" to request/go
 *   6. Go back to step 3 (ready for next test run)
 *
 */

#define TEST_DURATION_S 30
#define CLIENT_ID "publisher-01"
#define MQTT_TIMEOUT 10000L
#define SUB_QOS 0  

/*
 * Config received from analyser via request/# topics.
 * Volatile because they are written by the MQTT callback thread
 * and read by the main thread.
 */
static volatile int cfg_qos = 0;
static volatile int cfg_delay_ms = 100;
static volatile int cfg_msg_size = 1;
static volatile int g_start = 0;
static volatile int g_abort = 0;
static char g_start_nonce[32] = {0};

static MQTTClient g_client;

/*
 * Opens a new TSV file to record sent messages for this burst.
 * Filename: logs/<start_counter>_<iso_timestamp>.tsv
 *
 * The file stays open for the duration of the burst and one fwrite per published message. 
 * Close and flush when the burst ends.
 *
 * Returns FILE* on success, NULL on failure.
 */
static FILE *open_sent_tsv(int qos, int delay_ms, int msg_size) {
    const char *base_dir = "/out/publisher";
    if (mkdir(base_dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "publisher: could not create %s\n", base_dir);
        return NULL;
    }
 
    char path[256];
    snprintf(path, sizeof(path), 
        "%s/pq%d_d%d_s%d.tsv", 
        base_dir, qos, delay_ms, msg_size
    );
 
    FILE *fp = fopen(path, "a");
    if (fp == NULL) {
        fprintf(stderr, "publisher: failed to open TSV '%s'\n", path);
        return NULL;
    }
 
    fseek(fp, 0, SEEK_END);
    if (ftell(fp) == 0) {
        fprintf(fp, "counter\tpub_timestamp\ttopic\tmsg_size\tmqtt_success\n");
    }

    fprintf(stderr, "publisher: logging sent messages to %s\n", path);
    return fp;
}
 


/*
 * Publishes messages at the configured rate for TEST_DURATION_S seconds.
 *
 * For each message:
 *   1. Get timestamp
 *   2. Build payload: "counter:timestamp:xxx...xxx"
 *   3. Publish to broker
 *   4. On publish success: write row to TSV (ground truth)
 *   5. Sleep cfg_delay_ms if non-zero
 *
 * Only messages that MQTTClient_publish() accepts are written to TSV —
 * if publish fails, the counter is skipped and not recorded, so the
 * analyser will see a gap that is NOT counted as network loss.
 */
static void run_burst(void) {
    // Snapshot config at burst start to stay consistent for 30s 
    int qos      = cfg_qos;
    int delay_ms = cfg_delay_ms;
    int msg_size = cfg_msg_size;

    // Build topic: counter/<qos>/<delay>/<msg_size> 
    char topic[64];
    snprintf(topic, sizeof(topic), 
        "counter/%d/%d/%d",
        qos, delay_ms, msg_size
    );

    // Build x-string tail
    char *x_str = NULL;
    if (msg_size > 0) {
        x_str = malloc(msg_size + 1);
        if (x_str == NULL) {
            fprintf(stderr, "publisher: malloc failed for x_str\n");
            return;
        }
        memset(x_str, 'x', msg_size);
        x_str[msg_size] = '\0';
    }

    // Payload buffer: "counter:timestamp:xxx...xxx"
    // Max: 20 (counter) + 1 + 13 (ts) + 1 + 4000 (x) + null 
    int payload_buf_size = 20 + 1 + 13 + 1 + msg_size + 1;
    char *payload = malloc(payload_buf_size);
    if (payload == NULL) {
        fprintf(stderr, "publisher: malloc failed for payload\n");
        free(x_str); return;
    }

    FILE *tsv = open_sent_tsv(qos, delay_ms, msg_size);
    if (tsv == NULL) {
        free(x_str); free(payload); return;
    }

    long long counter = 0;
    time_t end_time = time(NULL) + TEST_DURATION_S;

    fprintf(stderr, "publisher: burst start. topic=%s qos=%d delay=%dms size=%d\n",
        topic, qos, delay_ms, msg_size
    );

    while (time(NULL) < end_time) {
        if (g_abort) {
            fprintf(stderr, "publisher: burst aborted by reset\n");
            g_abort = 0;
            break;
        }
        

        long long pub_ts = get_timestamp_ms();
        long long curr_count = counter; counter++;

        int payload_len;
        if (msg_size > 0) {
            payload_len = snprintf(payload, payload_buf_size, "%lld:%lld:%s", curr_count, pub_ts, x_str);
        } else {
            payload_len = snprintf(payload, payload_buf_size, "%lld:%lld:", curr_count, pub_ts);
        }

        int rc = MQTTClient_publish(g_client, topic, payload_len, payload, qos, 0, NULL);
        fprintf(tsv, "%lld\t%lld\t%s\t%d\t%d\n", curr_count, pub_ts, topic, msg_size, rc);

        if (rc != MQTTCLIENT_SUCCESS) {
            fprintf(stderr, "publisher: publish failed (rc=%d) for counter %lld\n", rc, curr_count);
        }

        if (delay_ms > 0) {
            usleep(delay_ms * 1000);
        }
    }

    fflush(tsv); fclose(tsv); 
    free(x_str); free(payload);

    fprintf(stderr, "publisher: burst done. Sent %lld messages\n", counter);
}

/*
 * Receives config from analyser via request/# topics.
 * Sets g_start=1 when "start" arrives on request/go.
 *
 * Intentionally minimal. No publishing or heavy work here since this runs on paho's internal callback thread.
 */
static int on_message(void *context, char *topic, int topic_len, MQTTClient_message *msg) {
    (void)context; (void)topic_len;

    // Null terminate payload copy (config values are short) */
    char payload[64];
    int len = msg->payloadlen < (int)sizeof(payload) - 1
            ? msg->payloadlen : (int)sizeof(payload) - 1;
    memcpy(payload, msg->payload, len);
    payload[len] = '\0';

    if (strcmp(topic, "request/qos") == 0) {
        cfg_qos = atoi(payload); fprintf(stderr, "publisher: config qos=%d\n", cfg_qos);

    } else if (strcmp(topic, "request/delay") == 0) {
        cfg_delay_ms = atoi(payload); fprintf(stderr, "publisher: config delay=%dms\n", cfg_delay_ms);

    } else if (strcmp(topic, "request/messagesize") == 0) {
        cfg_msg_size = atoi(payload); fprintf(stderr, "publisher: config msg_size=%d\n", cfg_msg_size);

    } else if (strcmp(topic, "request/go") == 0) {
        if (strncmp(payload, "start:", 6) == 0) {
            // Store the nonce so run_burst() can include it in "done"
            strncpy(g_start_nonce, payload + 6, sizeof(g_start_nonce) - 1);
            g_start_nonce[sizeof(g_start_nonce) - 1] = '\0';
            g_abort = 0;
            g_start = 1;
        }
  
        else if (strcmp(payload, "reset") == 0) {
        // Analyser gave up. abort current burst if running
        // Since run_burst() checks time(NULL) < end_time, set end_time to the past by flagging an abort
        g_abort = 1;
        fprintf(stderr, "publisher: reset received. Aborting burst\n");
        // Ignores "done". That's the analyser's own signal back to itself 
        }
    }

    MQTTClient_freeMessage(&msg); MQTTClient_free(topic);
    return 1;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <broker_address>\n", argv[0]);
        fprintf(stderr, "e.g: %s tcp://localhost:1883\n", argv[0]);
        return 1;
    }
    const char *broker = argv[1];

    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    conn_opts.keepAliveInterval = 20; 
    conn_opts.cleansession = 1;

    MQTTClient_create(&g_client, argv[1], CLIENT_ID, MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTClient_setCallbacks(g_client, NULL, NULL, on_message, NULL);

    int rc = MQTTClient_connect(g_client, &conn_opts);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "publisher: failed to connect to %s (rc=%d)\n", broker, rc);
        return 1;
    }
    fprintf(stderr, "publisher: connected to %s\n", broker);

    MQTTClient_subscribe(g_client, "request/#", SUB_QOS);
    fprintf(stderr, "publisher: subscribed to request/#.waiting for instructions\n");

    while (1) {
        if (g_start) {
            g_start = 0;
            run_burst();
            char done_payload[40];
            snprintf(done_payload, sizeof(done_payload), "done:%s", g_start_nonce);
            MQTTClient_publish(g_client, "request/go", (int)strlen(done_payload), done_payload, 2, 0, NULL);
            fprintf(stderr, "publisher: sent '%s' to request/go\n", done_payload);
            fprintf(stderr, "publisher: ready for next run\n");
        }
        usleep(10000);  // 10ms poll. Low CPU pressure, fast enough response
    }

    // run publisher until properly killed
    MQTTClient_disconnect(g_client, MQTT_TIMEOUT);
    MQTTClient_destroy(&g_client);
    return 0;
}