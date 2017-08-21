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
 * Hashtable consists of 2^24 rows where each of can have up to 256 slots.
 * A slot takes 12 bytes which equals to 98 bits. Where 32 bits are used for title hash,
 * 30 bits 28 bits for author last name hash, and 6 bits for author last name length.
 * Title hash hash consists of a hashtable row number and another 32 bits from slot.
 * The actual title hash keyspace is 24 + 32 = 56 bits.
 */

#include <stdio.h>
#include <stdint.h>
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
    uint32_t title_len = strlen(title);
    uint32_t name_len = strlen(name);

    if (name_len < 2) return 0;

    if (title_len < 10 || title_len > MAX_TITLE_LEN) return 0;

    if (!(title = text_transform(title, title_len))) {
        return 0;
    }

    static token_t tokens[MAX_TITLE_LEN];
    uint32_t tokens_len;
    text_tokens(title, tokens, &tokens_len);

    if (!(name = text_transform(name, name_len))) {
        return 0;
    }

    static token_t name_tokens[MAX_NAME_LEN];
    uint32_t name_tokens_len;
    text_tokens(name, name_tokens, &name_tokens_len);

    //print_ngram(title, tokens, 0, tokens_len);
    uint64_t hash = text_ngram_hash56(title, tokens, 0, tokens_len);

    //printf("Index hash: %" PRId64 "\n", hash);

    slot_t *slots[MAX_SLOTS_PER_TITLE];
    uint8_t slots_len;

    ht_hash_slots(hash, slots, &slots_len);

    uint64_t name_fingerprint;

    uint32_t extracted_name_start = name_tokens[name_tokens_len - 1].start;
    uint32_t extracted_name_len = name_tokens[name_tokens_len - 1].len;
    uint32_t name_hash28 = text_hash28(name + extracted_name_start, extracted_name_len);
    name_fingerprint = (((uint64_t) name_hash28) << 6) | extracted_name_len;

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
        free(title);
        free(name);
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

    free(title);
    free(name);
    gettimeofday(&t_updated, NULL);

//    indexed++;
//    if (indexed % 10000 == 0) {
//        printf("%d\n", indexed);
//    }

    return 1;
}

uint8_t *ht_locate_name(uint8_t *text, uint32_t text_len, uint32_t title_start,
                        uint32_t title_end, uint32_t name_hash28, uint8_t name_len) {
    int distance = 1000;
    int pos;

    pos = title_end + 1;
    while (pos + name_len < text_len + 1 && pos <= title_end + distance) {
        if (text_hash28(text + pos, name_len) == name_hash28) {
            return text + pos;
        }
        pos++;
    }

    pos = title_start - name_len;
    while (pos >= 0 && pos + distance >= title_start) {
        if (text_hash28(text + pos, name_len) == name_hash28) {
            return text + pos;
        }
        pos--;
    }

    return 0;
}

uint32_t ht_identify(uint8_t *text, result_t *result) {
    uint32_t text_len = strlen(text);
    if (!text_len) return 0;
    if (text_len > MAX_LOOKUP_TEXT_LEN) text_len = MAX_LOOKUP_TEXT_LEN;

    token_t tokens[MAX_LOOKUP_TEXT_LEN];
    uint32_t tokens_len;

    if (!(text = text_transform(text, text_len))) {
        return 0;
    }

    text_len = strlen(text);

    text_tokens(text, tokens, &tokens_len);

    line_t lines[MAX_LOOKUP_TEXT_LEN];
    uint32_t lines_len;
    text_lines(text, tokens, tokens_len, lines, &lines_len);

    uint32_t tried = 0;
    for (uint32_t i = 0; i < lines_len && tried <= 1000; i++) {
        for (uint32_t j = i; j < i + 5 && j < lines_len; j++) {
            uint32_t ngram_start = lines[i].start;
            uint32_t ngram_len = lines[j].start + lines[j].len - lines[i].start;
            uint32_t ngram_str_len = text_ngram_str_len(text, tokens, lines[i].start,
                                                        lines[j].start + lines[j].len - lines[i].start);

            // Title ngram must be at least 20 bytes len which results to about two normal length latin words or 5-7 chinese characters
            // Todo: Set different threshold for ASCI (transliterated..) characters and other characters
            if (ngram_str_len < 20 || ngram_str_len > 500) continue;
            tried++;
            uint64_t hash = text_ngram_hash56(text, tokens, ngram_start, ngram_len);
            //printf("Index hash: %" PRId64 "\n", hash);
            //print_ngram(text, tokens, lines[i].start, lines[j].start + lines[j].len - lines[i].start);

            slot_t *slots[MAX_SLOTS_PER_TITLE];
            uint8_t slots_len;
            ht_hash_slots(hash, slots, &slots_len);

            if (slots_len) {
                uint32_t id = 0;
                uint8_t *name = 0;
                uint8_t name_len = 0;
                for (uint32_t k = 0; k < slots_len; k++) {
                    uint32_t name_hash28 = (slots[k]->data >> 6) & 0xFFFFFFF;
                    name_len = slots[k]->data & 0x3F;
                    id = slots[k]->data >> 34;

                    name = ht_locate_name(text, text_len, tokens[ngram_start].start,
                                          tokens[ngram_start].start + ngram_len - 1,
                                          name_hash28, name_len);

                    if (name) break;
                }

                // If author name is found, or a title has at least 6 tokens, or a title is at least 30 bytes len
                if (name || ngram_len >= 6 || ngram_str_len >= 30) {
                    //print_ngram(text, tokens, lines[i].start, lines[j].start + lines[j].len - lines[i].start);
                    memset(result, 0, sizeof(result_t));

                    if (name) {
                        memcpy(result->name, name, name_len);
                        *(result->name + name_len) = 0;
                    }

                    text_ngram_str(text, tokens, lines[i].start, lines[j].start + lines[j].len - lines[i].start,
                                   result->title, sizeof(result->title));

                    if (id) {
                        db_get_identifiers(id, result->identifiers, sizeof(result->identifiers));
                    }

                    free(text);
                    return 1;
                }
            }
        }
    }

    free(text);
    return 0;
}
