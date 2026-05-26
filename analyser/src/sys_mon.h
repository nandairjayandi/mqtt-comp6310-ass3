#ifndef SYS_MON_H
#define SYS_MON_H

/*
 * $SYS topic recorder for the analyser
 *
 * Maintains a separate MQTT client subscribed to $SYS/# throughout the test suite. For each of the 54 test runs, it writes broker stats to a dedicated TSV file so they can be correlated with the message loss/ordering data captured by logger.c.
 *
 * TSV output format:
 *   recv_timestamp\ttopic\tvalue
 *   e.g. 1716200000123\t$SYS/broker/messages/received\t42
 *
 * Filename mirrors the logger convention:
 *   sys_pq{pub_qos}_sq{sub_qos}_d{delay}_s{msg_size}_{iso}.tsv
 */

#define SYS_DIR_MAX 256
#define SYS_VAL_MAX 128  
#define SYS_TOP_MAX 128

typedef struct {
    void *client;
    char output_dir[SYS_DIR_MAX]; char filepath[SYS_DIR_MAX + 80];
    int pub_qos;
    int sub_qos;
    int delay_ms;
    int msg_size;
} sys_mon_t;

/*
 * Connects MQTT client for $SYS subscription and creates the output directory.
 *
 * broker      : e.g. "tcp://broker:1883"
 * output_dir  : directory for sys TSV files (e.g. "logs/sys")
 *
 * Returns 0 on success, -1 on failure.
 */
int sys_mon_init(sys_mon_t *sm, const char *broker, const char *output_dir);

/*
 * Opens a new TSV file and starts recording $SYS messages into it. Closes the previous file if one was open.
 * Call before **each** test run. 
 */
void sys_mon_start(sys_mon_t *sm, int pub_qos, int sub_qos, int delay_ms, int msg_size);

/*
 * Closes the current TSV file. 
 * Call at the end of **each** test run.
 */
void sys_mon_stop(sys_mon_t *sm);

/*
 * Disconnects the MQTT client. 
 * Call once at the end of **all** tests.
 */
void sys_mon_destroy(sys_mon_t *sm);

#endif