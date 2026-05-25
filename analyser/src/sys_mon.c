#include "sys_mon.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>

#include <MQTTClient.h>

extern long long get_timestamp_ms(void);
extern void timestamp_to_iso(long long ms, char *buf, size_t buf_size);

static FILE *g_fp = NULL;
static sys_mon_t *g_self = NULL;  

static int on_sys_message(void *context, char *topic, int topic_len, MQTTClient_message *msg) {
    (void)context; (void)topic_len;

    if (g_fp == NULL) {
        MQTTClient_freeMessage(&msg); 
        MQTTClient_free(topic);
        return 1;
    }

    long long recv_ts = get_timestamp_ms();

    // Copy and null terminate $SYS values
    char value[SYS_VAL_MAX];
    int len = msg->payloadlen < (int)sizeof(value) - 1
            ? msg->payloadlen : (int)sizeof(value) - 1;
    memcpy(value, msg->payload, len);
    value[len] = '\0';

    fprintf(g_fp, "%lld\t%s\t%s\n", recv_ts, topic, value);

    MQTTClient_freeMessage(&msg);
    MQTTClient_free(topic);
    return 1;
}

int sys_mon_init(sys_mon_t *sm, const char *broker, const char *output_dir) {
    memset(sm, 0, sizeof(sys_mon_t));
    strncpy(sm->output_dir, output_dir, SYS_DIR_MAX - 1);
    g_self = sm;

    if (mkdir(output_dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "sys_mon: cannot create '%s': %s\n", output_dir, strerror(errno));
        return -1;
    }

    MQTTClient client;
    MQTTClient_connectOptions conn = MQTTClient_connectOptions_initializer;

    MQTTClient_create(&client, broker, "sys-mon-01", MQTTCLIENT_PERSISTENCE_NONE, NULL);
    MQTTClient_setCallbacks(client, NULL, NULL, on_sys_message, NULL);

    conn.keepAliveInterval = 20; conn.cleansession = 1;

    int rc = MQTTClient_connect(client, &conn);
    if (rc != MQTTCLIENT_SUCCESS) {
        fprintf(stderr, "sys_mon: failed to connect to %s (rc=%d)\n", broker, rc);
        return -1;
    }

    // subscribe $SYS at QoS 0; stats are informational only
    MQTTClient_subscribe(client, "$SYS/#", 0);
    printf("sys_mon: connected and subscribed to $SYS/#\n");

    sm->client = client;
    return 0;
}

void sys_mon_start(sys_mon_t *sm, int pub_qos, int sub_qos, int delay_ms, int msg_size) {
    // close previous test file if open
    if (g_fp != NULL) {
        fclose(g_fp);
        g_fp = NULL;
    }

    sm->pub_qos = pub_qos;
    sm->sub_qos = sub_qos;
    sm->delay_ms = delay_ms;
    sm->msg_size = msg_size;

    // sys_pq{n}_sq{n}_d{n}_s{n}.tsv 

    snprintf(sm->filepath, sizeof(sm->filepath),
        "%s/sys_pq%d_sq%d_d%d_s%d.tsv",
        sm->output_dir, pub_qos, sub_qos, delay_ms, msg_size
    );

    g_fp = fopen(sm->filepath, "w");
    if (g_fp == NULL) {
        fprintf(stderr, "sys_mon: failed to open '%s': %s\n",
                sm->filepath, strerror(errno));
        return;
    }

    fprintf(g_fp, "recv_timestamp\ttopic\tvalue\n");
    fflush(g_fp);

    printf("sys_mon: recording to %s\n", sm->filepath);
}

void sys_mon_stop(sys_mon_t *sm) {
    (void)sm;
    if (g_fp != NULL) {
        fflush(g_fp);
        fclose(g_fp);
        g_fp = NULL;
    }
}

void sys_mon_destroy(sys_mon_t *sm) {
    sys_mon_stop(sm);
    if (sm->client != NULL) {
        MQTTClient_disconnect((MQTTClient)sm->client, 5000);
        MQTTClient_destroy((MQTTClient *)&sm->client);
    }
}