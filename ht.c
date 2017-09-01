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

/*
 * Hashtable consists of 2^24 rows where each row can have up to 256 slots.
 * A slot takes 12 bytes which equals to 98 bits. Where 32 bits are used for title hash,
 * 30 bits for meta_id, 28 bits for author last name hash, and 6 bits for author last name length.
 * Title hash consists of a hashtable row number and another 32 bits from a slot.
 * The actual title hash keyspace is 24 + 32 = 56 bits.
 */

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <jemalloc/jemalloc.h>
#include "ht.h"
#include "db.h"
#include "text.h"

row_t rows[HASHTABLE_SIZE] = {0};
struct timeval t_updated = {0};
extern uint32_t last_meta_id;
//uint32_t indexed = 0;

uint32_t ht_init() {
    printf("loading hashtable..\n");
    if (!db_load_hashtable(rows)) {
        return 0;
    }
    return 1;
}

stats_t ht_stats() {
    stats_t stats = {0};
    for (uint32_t i = 0; i < HASHTABLE_SIZE; i++) {
        if (rows[i].len) stats.used_hashes++;
        stats.used_slots += rows[i].len;
        if (stats.max_slots < rows[i].len) stats.max_slots = rows[i].len;
    }

    for (uint32_t i = 0; i < HASHTABLE_SIZE; i++) {
        stats.slots_dist[rows[i].len]++;
    }

    return stats;
}

row_t *ht_row(uint64_t hash) {
    uint32_t hash24 = (uint32_t) (hash >> 32);
    return rows + hash24;
}

uint8_t ht_hash_slots(uint64_t hash, slot_t **slots, uint8_t *slots_len) {
    *slots_len = 0;
    uint32_t hash24 = (uint32_t) (hash >> 32);
    uint32_t hash32 = (uint32_t) (hash & 0xFFFFFFFF);
    row_t *row = &rows[hash24];
    if (row->len) {
        for (uint32_t i = 0; i < row->len; i++) {
            if (row->slots[i].hash32 == hash32) {
                slots[(*slots_len)++] = &row->slots[i];
            }
        }
    }
    return *slots_len;
}

uint32_t ht_add_slot(uint64_t hash, uint64_t data) {
    uint32_t hash23 = (uint32_t) (hash >> 32);
    uint32_t hash32 = (uint32_t) (hash & 0xFFFFFFFF);
    row_t *row = rows + hash23;

    if (row->len) {
        if (row->len >= ROW_SLOTS_MAX) {
            fprintf(stderr, "reached ROW_SLOTS_MAX limit");
            return 0;
        }
        if (!(row->slots = realloc(row->slots, sizeof(slot_t) * (row->len + 1)))) {
            fprintf(stderr, "slot realloc failed");
            return 0;
        };
        row->updated = 1;
    } else {
        if (!(row->slots = malloc(sizeof(slot_t)))) {
            fprintf(stderr, "slot malloc failed");
            return 0;
        }
        row->updated = 1;
    }

    slot_t *slot = row->slots + row->len;
    slot->hash32 = hash32;
    slot->data = data;

    row->len++;
    return 1;
}

uint32_t ht_index(uint8_t *title, uint8_t *name, uint8_t *identifiers) {
    char output_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;
    text_process(title, output_text, &output_text_len, 0, 0, 0, 0);

    uint8_t name_output[64];
    uint32_t name_output_len = 64;
    if (!text_process_name(name, name_output, &name_output_len)) return 0;

    if (name_output_len < 2) return 0;

    if (output_text_len < 10 || output_text_len > MAX_TITLE_LEN) return 0;

    uint64_t hash = text_hash56(output_text, output_text_len);
    printf("Index: %" PRId64 " %.*s\n", hash, output_text_len, output_text);

    slot_t *slots[MAX_SLOTS_PER_TITLE];
    uint8_t slots_len;

    ht_hash_slots(hash, slots, &slots_len);

    uint64_t name_fingerprint;
    uint32_t name_hash28 = text_hash28(name_output, name_output_len);
    name_fingerprint = (((uint64_t) name_hash28) << 6) | name_output_len;

    uint32_t slot_meta_id = 0;
    slot_t *slot = 0;
    for (uint32_t i = 0; i < slots_len; i++) {
        if ((slots[i]->data & 0x3FFFFFFFF) == name_fingerprint) {
            slot = slots[i];
            slot_meta_id = slots[i]->data >> 34;
            break;
        }
    }

    if (!slot && slots_len >= MAX_SLOTS_PER_TITLE) {
        fprintf(stderr, "reached MAX_SLOTS_PER_TITLE limit for title \"%s\"", title);
        return 0;
    }

    uint32_t new_meta_id = 0;
    if (!slot || !slot_meta_id) {
        new_meta_id = ++last_meta_id;
    }

    uint32_t meta_id = 0;
    if (slot_meta_id) {
        meta_id = slot_meta_id;
    } else {
        meta_id = new_meta_id;
    }

    uint32_t inserted = 0;

    if (identifiers) {
        uint8_t *p = identifiers;
        uint8_t *s;

        while (1) {
            while (*p == ',' || *p == ' ') p++;
            if (!*p) break;
            s = p;
            while (*p && *p != ',' && *p != ' ') p++;

            db_insert_identifier(meta_id, s, p - s);

            inserted++;

            if (!*p) break;
        }
    }

    if (!inserted) {
        // Don't set meta_id for titles that don't have any identifiers associated
        last_meta_id--;
        new_meta_id = 0;
    }

    if (!slot) {
        uint64_t data = (((uint64_t) new_meta_id) << 34) | name_fingerprint;
        ht_add_slot(hash, data);
    } else if (!slot_meta_id && new_meta_id) {
        slot->data = (((uint64_t) new_meta_id) << 34) | name_fingerprint;
        row_t *row = ht_row(hash);
        row->updated = 1;
    }

    gettimeofday(&t_updated, NULL);

//    indexed++;
//    if (indexed % 10000 == 0) {
//        printf("%d\n", indexed);
//    }

    return 1;
}

int32_t ht_locate_name(uint8_t *text, uint32_t text_len, uint32_t title_start,
                       uint32_t title_end, uint32_t name_hash28, uint8_t name_len) {
    int32_t distance = NAME_LOOKUP_DISTANCE;
    int32_t pos;

    pos = title_end + 1;
    while (pos + name_len < text_len + 1 && pos <= title_end + distance) {
        if (text_hash28(text + pos, name_len) == name_hash28) {
            return pos;
        }
        pos++;
    }

    pos = title_start - name_len;
    while (pos >= 0 && pos + distance >= title_start) {
        if (text_hash28(text + pos, name_len) == name_hash28) {
            return pos;
        }
        pos--;
    }

    return -1;
}

uint32_t ht_identify(uint8_t *text, result_t *result) {
    char output_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;

    uint32_t map[MAX_LOOKUP_TEXT_LEN];
    uint32_t map_len = MAX_LOOKUP_TEXT_LEN;

    line_t lines[MAX_LOOKUP_TEXT_LEN];
    uint32_t lines_len = MAX_LOOKUP_TEXT_LEN;

    text_process(text, output_text, &output_text_len, map, &map_len, lines, &lines_len);

    uint32_t tried = 0;
    for (uint32_t i = 0; i < lines_len && tried <= 1000; i++) {
        for (uint32_t j = i; j < i + 5 && j < lines_len; j++) {

            uint32_t title_start = lines[i].start;
            uint32_t title_end = lines[j].end;
            uint32_t title_len = title_end - title_start + 1;

            // Title ngram must be at least 20 bytes len which results to about two normal length latin words or 5-7 chinese characters
            // Todo: Set a different threshold for ASCI (and transliterated) characters and other characters
            if (title_len < 20 || title_len > 500) continue;

            tried++;
            uint64_t hash = text_hash56(output_text + title_start, title_end - title_start + 1);
            //printf("Lookup: %" PRId64 " %.*s\n", hash, title_end-title_start+1, output_text+title_start);

            slot_t *slots[MAX_SLOTS_PER_TITLE];
            uint8_t slots_len;
            ht_hash_slots(hash, slots, &slots_len);

            if (slots_len) {
                uint32_t id = 0;
                int32_t name_pos = 0;
                uint8_t name_len = 0;
                for (uint32_t k = 0; k < slots_len; k++) {
                    uint32_t name_hash28 = (slots[k]->data >> 6) & 0xFFFFFFF;
                    name_len = slots[k]->data & 0x3F;
                    id = slots[k]->data >> 34;

                    name_pos = ht_locate_name(output_text, output_text_len, title_start, title_end,
                                              name_hash28, name_len);
                    if (name_pos) break;
                }

                // TODO: If author name is found, or a title has at least 6 tokens, or a title is at least 30 bytes len
                if (name_pos>=0 || title_len >= 40) {
                    //print_ngram(text, tokens, lines[i].start, lines[j].start + lines[j].len - lines[i].start);
                    memset(result, 0, sizeof(result_t));

                    if (name_pos>=0) {
                        text_original_name(text, map, map_len, name_pos, name_pos + name_len - 1,
                                           result->name, sizeof(result->name));
                    }

                    text_original_str(text, map, map_len, title_start, title_end,
                                      result->title, sizeof(result->title));

                    if (id) {
                        db_get_identifiers(id, result->identifiers, sizeof(result->identifiers));
                    }

                    return 1;
                }
            }
        }
    }

    return 0;
}
