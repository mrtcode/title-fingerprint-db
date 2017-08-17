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
#include <stdint.h>
#include <unicode/ustdio.h>
#include <unicode/ubrk.h>
#include <unicode/ustring.h>
#include <string.h>
#include <sys/time.h>
#include <jemalloc/jemalloc.h>

#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

#include "hashtable.h"
#include "db.h"

row_t rows[HASHTABLE_SIZE] = {0};
struct timeval t_updated = {0};
extern uint32_t last_meta_id;
//uint32_t indexed = 0;

UTransliterator *transliterator;
UConverter *converter;

stats_t get_stats() {
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

uint32_t init_icu() {
    UErrorCode status = U_ZERO_ERROR;

    char str[] = "NFD; [:Symbol:] Remove; [:Nonspacing Mark:] Remove; NFKD; Lower;";
    UChar ustr[256];
    uint32_t ustr_len = 0;
    u_strFromUTF8(ustr, sizeof(ustr), &ustr_len, str, strlen(str), &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "u_strFromUTF8 failed, error=%s\n", u_errorName(status));
        return 0;
    }

    transliterator = utrans_openU(ustr, ustr_len, UTRANS_FORWARD, NULL, 0, NULL, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "utrans_openU failed, error=%s\n", u_errorName(status));
        return 0;
    }

    converter = ucnv_open("UTF-8", &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "ucnv_open failed, error=%s\n", u_errorName(status));
        return 0;
    }
    return 1;
}

uint8_t *process_text(uint8_t *text, uint32_t text_len, token_t *tokens, uint32_t *tokens_len) {
    UErrorCode status = U_ZERO_ERROR;

    uint32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(converter));
    UChar *uc1;
    if (!(uc1 = malloc(target_len))) {
        fprintf(stderr, "uc1 malloc failed");
        return 0;
    }

    ucnv_toUChars(converter, uc1, target_len, text, text_len, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "ucnv_toUChars failed, error=%s\n", u_errorName(status));
        return 0;
    }

    uint32_t limit = u_strlen(uc1);
    utrans_transUChars(transliterator, uc1, 0, target_len, 0, &limit, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "utrans_transUChars failed, error=%s\n", u_errorName(status));
        return 0;
    }

    char *target;
    if (!(target = malloc(target_len))) {
        fprintf(stderr, "target malloc failed");
        return 0;
    }

    ucnv_fromUChars(converter, target, target_len, uc1, limit, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "ucnv_fromUChars failed, error=%s\n", u_errorName(status));
        return 0;
    }


    UBreakIterator *bi = NULL;
    bi = ubrk_open(UBRK_WORD, 0, NULL, 0, &status);
    if (status != U_ZERO_ERROR) {
        //fprintf(stderr, "ubrk_open failed, error=%s\n", u_errorName(status));
        //return 0;
    }

    status = U_ZERO_ERROR;

    *tokens_len = 0;

    ubrk_setText(bi, uc1, limit, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "ubrk_setText failed, error=%s\n", u_errorName(status));
        return 0;
    }
    uint32_t start = 0, pos;
    while ((pos = ubrk_next(bi)) != UBRK_DONE) {
        if (ubrk_getRuleStatus(bi) != UBRK_WORD_NONE) {
            tokens[*tokens_len].start = start;
            tokens[*tokens_len].len = pos - start;
            (*tokens_len)++;
        }
        start = pos;
    }

    ubrk_close(bi);

    free(uc1);

    return target;
}

uint64_t get_ngram_hash56(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);

    for (uint32_t i = start; i < start + len; i++) {
        token_t token = tokens[i];
        XXH64_update(&state64, text + token.start, token.len);
    }
    return (XXH64_digest(&state64)) >> 8;
}

uint8_t get_hash_slots(uint64_t hash, slot_t *slots, uint8_t *slots_len) {
    *slots_len = 0;
    uint32_t hash24 = (uint32_t) (hash >> 32);
    uint32_t hash32 = (uint32_t) (hash & 0xFFFFFFFF);
    row_t *row = &rows[hash24];
    if (row->len) {
        for (uint32_t i = 0; i < row->len; i++) {
            if (row->slots[i].hash32 == hash32) {
                slots[(*slots_len)++] = row->slots[i];
            }
        }
    }
    return *slots_len;
}

uint32_t add_slot(uint64_t hash, uint64_t data) {
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

void print_ngram(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len) {
    for (uint32_t i = start; i < start + len; i++) {
        token_t token = tokens[i];
        printf("%.*s ", token.len, &text[token.start]);
    }
    printf("\n");
}

uint8_t *get_ngram_str(uint8_t *text, token_t *tokens, uint32_t ngram_start,
                       uint32_t ngram_len, uint8_t *str, uint32_t str_len_max) {
    uint32_t k = 0;
    uint32_t ngram_end = ngram_start + ngram_len - 1;
    for (uint32_t i = ngram_start; i <= ngram_end; i++) {
        uint32_t start = tokens[i].start;
        uint32_t end = tokens[i].start + tokens[i].len - 1;
        for (uint32_t j = start; j <= end && k < str_len_max - 1; j++) {
            *(str + k) = text[j];
            k++;
        }
        if (k >= str_len_max - 1) return 0;
        if (i == ngram_end) break;
        *(str + k) = ' ';
        k++;
    }
    *(str + k) = 0;
    return str;
}

uint32_t get_name_hash28(uint8_t *name, uint8_t name_len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);
    XXH64_update(&state64, name, name_len);
    return (uint32_t) (XXH64_digest(&state64) & 0xFFFFFFF);
}

uint32_t index_title(uint8_t *title, uint8_t *name, uint8_t *identifiers) {
    uint32_t title_len = strlen(title);
    uint32_t name_len = strlen(name);

    if (name_len < 2) return 0;

    if (title_len > MAX_TITLE_LEN) return 0;

    static token_t tokens[MAX_TITLE_LEN];
    uint32_t tokens_len;

    if (!(title = process_text(title, title_len, tokens, &tokens_len))) {
        return 0;
    }

    static token_t name_tokens[MAX_NAME_LEN];
    uint32_t name_tokens_len;

    if (!(name = process_text(name, name_len, name_tokens, &name_tokens_len))) {
        return 0;
    }

    //print_ngram(title, tokens, 0, tokens_len);
    uint64_t hash = get_ngram_hash56(title, tokens, 0, tokens_len);

    //printf("Index hash: %" PRId64 "\n", hash);

    slot_t slots[MAX_SLOTS_PER_TITLE];
    uint8_t slots_len;

    get_hash_slots(hash, slots, &slots_len);

    uint64_t name_fingerprint;

    uint32_t extracted_name_start = name_tokens[name_tokens_len - 1].start;
    uint32_t extracted_name_len = name_tokens[name_tokens_len - 1].len;
    uint32_t name_hash28 = get_name_hash28(name + extracted_name_start, extracted_name_len);
    name_fingerprint = (((uint64_t) name_hash28) << 6) | extracted_name_len;

    uint32_t meta_id = 0;
    uint8_t meta_exists = 0;
    for (uint32_t i = 0; i < slots_len; i++) {
        if ((slots[i].data & 0x3FFFFFFFF) == name_fingerprint) {
            meta_exists = 1;
            meta_id = slots[i].data >> 34;
            break;
        }
    }

    if (!meta_id) {
        if (slots_len >= MAX_SLOTS_PER_TITLE) {
            free(title);
            free(name);
            fprintf(stderr, "reached MAX_SLOTS_PER_TITLE limit for title=\"%s\"", title);
            return 0;
        }
        meta_id = ++last_meta_id;
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

    if (!meta_exists) {
        if (!inserted) {
            // Don't set meta_id for titles that don't have any identifiers associated
            last_meta_id--;
            meta_id = 0;
        }

        uint64_t data = (((uint64_t) meta_id) << 34) | name_fingerprint;
        add_slot(hash, data);
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

uint8_t *locate_name(uint8_t *text, uint32_t text_len, uint32_t title_start,
                     uint32_t title_end, uint32_t name_hash28, uint8_t name_len) {
    int distance = 1000;
    int pos;

    pos = title_end + 1;
    while (pos + name_len < text_len + 1 && pos <= title_end + distance) {
        if (get_name_hash28(text + pos, name_len) == name_hash28) {
            return text + pos;
        }
        pos++;
    }

    pos = title_start - name_len;
    while (pos >= 0 && pos + distance >= title_start) {
        if (get_name_hash28(text + pos, name_len) == name_hash28) {
            return text + pos;
        }
        pos--;
    }

    return 0;
}

uint32_t get_lines(uint8_t *text, token_t *tokens, uint32_t tokens_len, line_t *lines, uint32_t *lines_len) {
    uint32_t line_nr = 0;
    lines[line_nr].start = 0;
    lines[line_nr].len = 0;
    for (uint32_t i = 0; i < tokens_len; i++) {
        lines[line_nr].len++;

        if (i < tokens_len - 1) {
            uint8_t *p = text + tokens[i].start + tokens[i].len;
            uint8_t *e = text + tokens[i + 1].start;

            while (p < e) {
                if (*p == '\n' || *p == '\r') {
                    line_nr++;
                    lines[line_nr].start = i + 1;
                    lines[line_nr].len = 0;
                    break;
                }
                p++;
            }
        }
    }

    *lines_len = line_nr + 1;
    return 0;
}

uint32_t identify(uint8_t *text, result_t *result) {
    uint32_t text_len = strlen(text);
    if (text_len > MAX_LOOKUP_TEXT_LEN) text_len = MAX_LOOKUP_TEXT_LEN;

    token_t tokens[MAX_LOOKUP_TEXT_LEN];
    uint32_t tokens_len;

    if (!(text = process_text(text, text_len, tokens, &tokens_len))) {
        return 0;
    }

    text_len = strlen(text);

    line_t lines[MAX_LOOKUP_TEXT_LEN];
    uint32_t lines_len;

    get_lines(text, tokens, tokens_len, lines, &lines_len);

    uint32_t tried = 0;
    for (uint32_t i = 0; i < lines_len && tried <= 1000; i++) {
        for (uint32_t j = i; j < i + 6 && j < lines_len; j++) {

            uint32_t start = lines[i].start;
            uint32_t len = lines[j].start + lines[j].len - lines[i].start;

            // Between 2 and 70 tokens
            if (len < 2 || len > 70) continue;
            tried++;
            uint64_t hash = get_ngram_hash56(text, tokens, start, len);
            //printf("Index hash: %" PRId64 "\n", hash);
            //print_ngram(text, tokens, lines[i].start, lines[j].start + lines[j].len - lines[i].start);

            slot_t slots[MAX_SLOTS_PER_TITLE];
            uint8_t slots_len;
            get_hash_slots(hash, slots, &slots_len);

            if (slots_len) {
                uint32_t id = 0;
                uint8_t *name = 0;
                uint8_t name_len = 0;
                for (uint32_t k = 0; k < slots_len; k++) {

                    uint32_t name_hash28 = (slots[k].data >> 6) & 0xFFFFFFF;
                    name_len = slots[k].data & 0x3F;
                    id = slots[k].data >> 34;

                    name = locate_name(text, text_len, tokens[start].start, tokens[start].start + len - 1,
                                       name_hash28, name_len);

                    if (name) break;
                }

                // If author name is found or a title has at least 6 tokens
                if (name || len >= 6) {
                    //print_ngram(text, tokens, lines[i].start, lines[j].start + lines[j].len - lines[i].start);

                    memset(result, 0, sizeof(result_t));

                    if (name) {
                        memcpy(result->name, name, name_len);
                        *(result->name + name_len) = 0;
                    }

                    get_ngram_str(text, tokens, lines[i].start, lines[j].start + lines[j].len - lines[i].start,
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
    return 1;
}

uint32_t load() {
    db_load_hashtable(rows);
}
