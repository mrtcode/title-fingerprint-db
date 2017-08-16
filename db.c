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

#include <sqlite3.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <linux/limits.h>
#include "hashtable.h"
#include "db.h"

char db_path_hashtable[PATH_MAX];
char db_path_identifiers[PATH_MAX];


sqlite3 *sqlite;
sqlite3 *sqlite_ids;
sqlite3 *sqlite_ids_read;

uint32_t last_meta_id = 0;
uint32_t identifiers_in_transaction = 0;
sqlite3_stmt *insert_stmt = 0;

int db_init(char *directory) {
    int rc;

    snprintf(db_path_hashtable, sizeof(db_path_hashtable), "%s/hashtable.sqlite", directory);
    snprintf(db_path_identifiers, sizeof(db_path_hashtable), "%s/identifiers.sqlite", directory);

    if ((rc=sqlite3_config(SQLITE_CONFIG_SERIALIZED)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_config: (%i)\n", rc);
        return 0;
    }

    if(!db_init_hashtable()) {
        return 0;
    }

    if(!db_init_identifiers()) {
        return 0;
    }

    return 1;
}

int db_close() {
    int rc;
    if ((rc = sqlite3_finalize(insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_ids)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_ids_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite));
        return 0;
    }
    return 1;
}

int db_init_hashtable() {
    char *sql;
    int rc;
    char *err_msg;
    if ((rc = sqlite3_open(db_path_hashtable, &sqlite)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", db_path_hashtable, rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS hashtable (id INTEGER PRIMARY KEY, data BLOB);";
    if ((rc = sqlite3_exec(sqlite, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: (%d): %s\n", rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    return 1;
}

int db_init_identifiers() {
    int rc;
    char *err_msg = 0;

    if ((rc = sqlite3_open(db_path_identifiers, &sqlite_ids)) != SQLITE_OK) {
        fprintf(stderr, "Cannot open database %s (%d): %s\n", db_path_identifiers, rc, sqlite3_errmsg(sqlite_ids));
        return 0;
    }

    sqlite3_exec(sqlite_ids, "PRAGMA journal_mode = WAL;", NULL, 0, 0);

    char *sql = "CREATE TABLE IF NOT EXISTS identifiers (meta_id INTEGER, identifier TEXT)";
    if ((rc = sqlite3_exec(sqlite_ids, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "SQL error (%d): %s\n", rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_meta_id_identifier ON identifiers (meta_id, identifier)";
    if ((rc = sqlite3_exec(sqlite_ids, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "SQL error (%d): %s\n", rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE INDEX IF NOT EXISTS idx_meta_id ON identifiers (meta_id)";
    if ((rc = sqlite3_exec(sqlite_ids, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "SQL error (%d): %s\n", rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }


    sql = "SELECT MAX(meta_id) FROM identifiers";
    sqlite3_stmt *stmt = NULL;

    if ((rc = sqlite3_prepare_v2(sqlite_ids, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ids));
        return 0;
    }

    uint32_t id = 0;
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        id = sqlite3_column_int(stmt, 0);
    }

    last_meta_id = id;

    sqlite3_finalize(stmt);

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ids, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ids));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO identifiers (meta_id,identifier) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_ids, sql, -1, &insert_stmt, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ids));
        return 0;
    }

    if ((rc = sqlite3_open(db_path_identifiers, &sqlite_ids_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", db_path_identifiers, rc, sqlite3_errmsg(sqlite_ids_read));
        return 0;
    }

    return 1;
}

int db_save_identifiers() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if (sqlite3_exec(sqlite_ids, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ids));
        sqlite3_free(err_msg);
        return 0;
    }

//    if ((rc = sqlite3_wal_checkpoint(sqlite_ids, NULL)) != SQLITE_OK) {
//        fprintf(stderr, "sqlite3_wal_autocheckpoint: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ids));
//        return 0;
//    }

    sql = "BEGIN TRANSACTION";
    if (sqlite3_exec(sqlite_ids, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ids));
        sqlite3_free(err_msg);
        return 0;
    }
    identifiers_in_transaction = 0;
}

int db_insert_identifier(uint32_t meta_id, uint8_t *identifier, uint32_t identifier_len) {
    int rc;

    if ((rc=sqlite3_bind_int(insert_stmt, 1, meta_id)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ids));
        return 0;
    }

    if ((rc=sqlite3_bind_text(insert_stmt, 2, identifier, identifier_len, SQLITE_STATIC)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_text: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ids));
        return 0;
    }

    if ((rc = sqlite3_step(insert_stmt)) != SQLITE_DONE) {
        fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    if ((rc = sqlite3_clear_bindings(insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_clear_bindings: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    if ((rc = sqlite3_reset(insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    identifiers_in_transaction++;

    if (identifiers_in_transaction > 5000000) {
        //di_save();
    }
}

int db_get_identifiers(uint32_t id, uint8_t *identifiers, uint32_t identifiers_max_len) {
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT GROUP_CONCAT(identifier) AS identifiers FROM identifiers WHERE meta_id = ? LIMIT 50";
    if ((rc = sqlite3_prepare_v2(sqlite_ids_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ids_read));
        return 0;
    }

    if ((rc=sqlite3_bind_int(stmt, 1, id)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ids_read));
        return 0;
    }

    uint32_t dis_len = 0;

    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint8_t *row_identifiers = sqlite3_column_blob(stmt, 0);
        uint32_t row_identifiers_len = sqlite3_column_bytes(stmt, 0);
        if(row_identifiers_len>identifiers_max_len-1) {
            row_identifiers_len = identifiers_max_len-1;
        }
        memcpy(identifiers, row_identifiers, row_identifiers_len);
        *(identifiers + row_identifiers_len) = 0;
    }

    printf("%s\n", identifiers);

    sqlite3_finalize(stmt);
    return 0;
}


int save_hashtable(row_t *rows, uint32_t rows_len) {
    char *sql;
    char *err_msg;
    int rc;

    sql = "INSERT OR REPLACE INTO hashtable (id, data) VALUES (?,?);";
    sqlite3_stmt *stmt = NULL;
    if ((rc = sqlite3_prepare_v2(sqlite, sql, -1, &stmt, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if (sqlite3_exec(sqlite, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite));
        sqlite3_free(err_msg);
        return 0;
    }

    for (int i = 0; i < rows_len; i++) {
        row_t *row = &rows[i];

        if (!row->updated) continue;
        row->updated = 0;

        if ((rc = sqlite3_bind_int(stmt, 1, i)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_bind_int: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
            return 0;
        }

        if ((rc = sqlite3_bind_blob(stmt, 2, row->slots, sizeof(slot_t) * row->len, SQLITE_STATIC)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_bind_blob: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
            return 0;
        }

        if ((rc = sqlite3_step(stmt)) != SQLITE_DONE) {
            fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
            return 0;
        }

        if ((rc = sqlite3_clear_bindings(stmt)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_clear_bindings: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
            return 0;
        }

        if ((rc = sqlite3_reset(stmt)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
            return 0;
        }
    }

    sql = "END TRANSACTION";
    if (sqlite3_exec(sqlite, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    sqlite3_finalize(stmt);
    return 1;
}

int load_hashtable(row_t *rows) {
    int rc;
    const char sql[] = "SELECT id, data FROM hashtable";
    sqlite3_stmt *stmt = NULL;

    if ((rc = sqlite3_prepare_v2(sqlite, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (sqlite3_column_count(stmt) == 2) {
            uint32_t id = sqlite3_column_int(stmt, 0);
            uint8_t *data = sqlite3_column_blob(stmt, 1);
            int len = sqlite3_column_bytes(stmt, 1);

            if (id >= HASHTABLE_SIZE) continue;

            rows[id].len = len / sizeof(slot_t);
            rows[id].slots = malloc(len);
            memcpy(rows[id].slots, data, len);
        }
    }

    if (SQLITE_DONE != rc) {
        fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(sqlite));
    }

    sqlite3_finalize(stmt);
    return 1;
}
