/*
 ***** BEGIN LICENSE BLOCK *****

 Copyright © 2017 Zotero
 https://www.zotero.org

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 ***** END LICENSE BLOCK *****
 */

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include <onion/onion.h>
#include <onion/block.h>
#include <onion/exportlocal.h>
#include <jansson.h>
#include <unicode/utf.h>
#include <unicode/utypes.h>
#include "ht.h"
#include "db.h"
#include "text.h"

extern row_t rows[HASHTABLE_SIZE];
extern struct timeval t_updated;
extern uint32_t identifiers_in_transaction;

onion *on = NULL;
pthread_rwlock_t rwlock;

onion_connection_status url_identify(void *_, onion_request *req, onion_response *res) {
    if (!(onion_request_get_flags(req) & OR_POST)) {
        return OCS_PROCESSED;
    }

    const onion_block *dreq = onion_request_get_data(req);

    if (!dreq) return OCS_PROCESSED;

    const char *data = onion_block_data(dreq);

    json_t *root;
    json_error_t error;
    root = json_loads(data, 0, &error);

    if (!root || !json_is_object(root)) {
        return OCS_PROCESSED;
    }

    json_t *json_text = json_object_get(root, "text");

    if (!json_is_string(json_text)) {
        return OCS_PROCESSED;;
    }

    uint8_t *text = json_string_value(json_text);

    struct timeval st, et;

    result_t result;
    uint32_t rc;
    pthread_rwlock_rdlock(&rwlock);

    gettimeofday(&st, NULL);
    rc = ht_identify(text, &result);
    gettimeofday(&et, NULL);

    pthread_rwlock_unlock(&rwlock);

    uint32_t elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec);

    json_t *obj = json_object();

    if (rc) {
        json_object_set_new(obj, "time", json_integer(elapsed));
        json_object_set(obj, "title", json_string(result.title));
        json_object_set(obj, "name", json_string(result.name));
        json_object_set(obj, "identifiers", json_string(result.identifiers));
    } else {

    }

    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, str);
    free(str);

    return OCS_PROCESSED;
}

onion_connection_status url_index(void *_, onion_request *req, onion_response *res) {
    if (onion_request_get_flags(req) & OR_POST) {
        struct timeval st, et;

        gettimeofday(&st, NULL);
        const onion_block *dreq = onion_request_get_data(req);

        if (!dreq) return OCS_PROCESSED;

        const char *data = onion_block_data(dreq);

        json_t *root;
        json_error_t error;

        root = json_loads(data, 0, &error);

        if (!root) {
            return OCS_PROCESSED;
        }

        uint32_t indexed = 0;
        pthread_rwlock_wrlock(&rwlock);
        if (json_is_array(root)) {
            uint32_t n = (uint32_t) json_array_size(root);
            int i;
            for (i = 0; i < n; i++) {
                json_t *el = json_array_get(root, i);
                if (json_is_object(el)) {
                    json_t *json_title = json_object_get(el, "title");
                    json_t *json_name = json_object_get(el, "name");
                    json_t *json_identifiers = json_object_get(el, "identifiers");
                    uint8_t *title = json_string_value(json_title);
                    uint8_t *name = json_string_value(json_name);
                    uint8_t *identifiers = json_string_value(json_identifiers);
                    if (ht_index(title, name, identifiers))
                        indexed++;
                }
            }
        }
        pthread_rwlock_unlock(&rwlock);

        json_decref(root);

        gettimeofday(&et, NULL);

        json_t *obj = json_object();
        json_object_set_new(obj, "indexed", json_integer(indexed));

        char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
        json_decref(obj);

        onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
        onion_response_printf(res, str);
        free(str);
    }

    return OCS_PROCESSED;;
}

onion_connection_status url_stats(void *_, onion_request *req, onion_response *res) {
    stats_t stats = ht_stats();
    json_t *obj = json_object();
    json_object_set(obj, "used_hashes", json_integer(stats.used_hashes));
    json_object_set(obj, "used_slots", json_integer(stats.used_slots));
    json_object_set(obj, "max_slots", json_integer(stats.max_slots));

    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, str);
    free(str);

    return OCS_PROCESSED;
}

int save() {
    printf("saving..\n");
    pthread_rwlock_rdlock(&rwlock);
    db_save_identifiers();
    db_save_hashtable(rows, HASHTABLE_SIZE);
    pthread_rwlock_unlock(&rwlock);
    printf("..saved\n");
}

void *saver_thread(void *arg) {
    struct timeval t_current;

    while (1) {
        usleep(10000);

        // Force flush because otherwise Docker doesn't output logs
        fflush(stdout);
        fflush(stderr);

        if (!t_updated.tv_sec) {
            continue;
        }

        gettimeofday(&t_current, NULL);
        if (t_current.tv_sec - t_updated.tv_sec >= 10
            || identifiers_in_transaction > 50000000) {
            save();
            t_updated.tv_sec = 0;
            t_updated.tv_usec = 0;
        }
    }
}

void signal_handler(int signum) {
    fprintf(stderr, "\nsignal received (%d), shutting down..\n", signum);

    if (on) {
        onion_listen_stop(on);
    }

    save();
    if (!db_close()) {
        fprintf(stderr, "db close failed\n");
        return;
    }

    fprintf(stderr, "exit\n");

    // Force flush because otherwise Docker doesn't output logs
    fflush(stdout);
    fflush(stderr);
    exit(EXIT_SUCCESS);
}

void print_usage() {
    printf("Missing parameters.\nUsage example:\ntitle-fingerprint-db -d /var/db -p 8080\n");
}

int main(int argc, char **argv) {
    char *opt_db_directory = 0;
    char *opt_port = 0;

    int opt;
    while ((opt = getopt(argc, argv, "d:p:")) != -1) {
        switch (opt) {
            case 'd':
                opt_db_directory = optarg;
                break;
            case 'p':
                opt_port = optarg;
                break;
            default:
                print_usage();
                return EXIT_FAILURE;
        }
    }

    if (!opt_db_directory || !opt_port) {
        print_usage();
        return EXIT_FAILURE;
    }

    setenv("ONION_LOG", "noinfo", 1);
    pthread_rwlock_init(&rwlock, 0);

    if (!text_init()) {
        fprintf(stderr, "failed to initialize text processor\n");
        return EXIT_FAILURE;
    }

    if (!db_init(opt_db_directory)) {
        fprintf(stderr, "failed to initialize db\n");
        return EXIT_FAILURE;
    }

    if (!ht_init()) {
        fprintf(stderr, "failed to initialize hashtable\n");
        return EXIT_FAILURE;
    }

    stats_t stats = ht_stats();
    printf("used_hashes=%u, used_slots=%u, max_slots=%u\n",
           stats.used_hashes, stats.used_slots, stats.max_slots);


    pthread_t tid;
    pthread_create(&tid, NULL, saver_thread, 0);


    on = onion_new(O_POOL);

    // Signal handler must be initialized after onion_new
    struct sigaction action;
    memset(&action, 0, sizeof(struct sigaction));
    action.sa_handler = signal_handler;
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);

    onion_set_port(on, opt_port);
    onion_set_max_threads(on, 16);
    onion_set_max_post_size(on, 50000000);

    onion_url *urls = onion_root_url(on);

    onion_url_add(urls, "identify", url_identify);
    onion_url_add(urls, "index", url_index);
    onion_url_add(urls, "stats", url_stats);
    onion_url_add_handler(urls, "panel", onion_handler_export_local_new("static/panel.html"));

    printf("listening on port %s\n", opt_port);

    onion_listen(on);

    onion_free(on);
    return EXIT_SUCCESS;
}
