/* syncsrv.c - generate scheduling trigger */

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <libgen.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/param.h>
#include <stdbool.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <ctype.h>
#include <zmq.h>
#include <czmq.h>
#include <json/json.h>

#include "zmsg.h"
#include "route.h"
#include "cmbd.h"
#include "log.h"
#include "plugin.h"

#define MAX_SYNC_PERIOD_SEC 30*60

static int epoch = 0;

static void _timeout (plugin_ctx_t *p)
{
    plugin_send_event (p, "event.sched.trigger.%d", ++epoch);
}

static void _set_sync_period_sec (const char *key, double val, void *arg,
                                  int errnum)
{
    plugin_ctx_t *p = arg;

    if (errnum != 0) {
        errn (errnum, "sync: %s", key);
        plugin_timeout_clear (p);
    } else if (val == NAN || val <= 0 || val > MAX_SYNC_PERIOD_SEC) {
        msg ("sync: %s: bad value (%f)", key, val);
        plugin_timeout_clear (p);
    } else 
        plugin_timeout_set (p, (int)(val * 1000)); /* msec */
}

static void _init (plugin_ctx_t *p)
{
    kvs_watch_double (p, "conf.sync.period-sec", _set_sync_period_sec, p);
}

struct plugin_struct syncsrv = {
    .name      = "sync",
    .initFn    = _init,
    .timeoutFn = _timeout,
};

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
