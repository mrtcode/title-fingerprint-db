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

#include <jemalloc/jemalloc.h>
#include <unicode/ustdio.h>
#include <unicode/ustring.h>
#include <string.h>

#define XXH_STATIC_LINKING_ONLY

#include "xxhash.h"
#include "text.h"

UTransliterator *text_transliterator;
UConverter *text_converter;

uint32_t text_init() {
    UErrorCode status = U_ZERO_ERROR;

    UChar ustr[256];
    u_snprintf(ustr, sizeof(ustr), "NFD; [:Symbol:] Remove; [:Nonspacing Mark:] Remove; NFKD; Lower;");

    text_transliterator = utrans_openU(ustr, u_strlen(ustr), UTRANS_FORWARD, NULL, 0, NULL, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "utrans_openU failed, error=%s\n", u_errorName(status));
        return 0;
    }

    text_converter = ucnv_open("UTF-8", &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "ucnv_open failed, error=%s\n", u_errorName(status));
        return 0;
    }
    return 1;
}

uint8_t *text_transform(uint8_t *text, uint32_t text_len) {
    UErrorCode status = U_ZERO_ERROR;
    UChar *uc;
    int32_t uc_len = text_len * 2 * 2;
    if (!(uc = malloc(uc_len))) {
        fprintf(stderr, "uc malloc failed");
        return 0;
    }

    ucnv_toUChars(text_converter, uc, uc_len, text, text_len, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "ucnv_toUChars failed, error=%s\n", u_errorName(status));
        return 0;
    }

    int32_t limit = u_strlen(uc);
    utrans_transUChars(text_transliterator, uc, 0, uc_len, 0, &limit, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "utrans_transUChars failed, error=%s\n", u_errorName(status));
        return 0;
    }

    uint8_t *text2;
    int32_t text2_len = UCNV_GET_MAX_BYTES_FOR_STRING(limit, ucnv_getMaxCharSize(text_converter));
    if (!(text2 = malloc(text2_len))) {
        fprintf(stderr, "text2 malloc failed");
        return 0;
    }

    ucnv_fromUChars(text_converter, text2, text2_len, uc, limit, &status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "ucnv_fromUChars failed, error=%s\n", u_errorName(status));
        return 0;
    }

    free(uc);
    return text2;
}

int text_tokens(uint8_t *text, token_t *tokens, uint32_t *tokens_len) {
    *tokens_len = 0;

    uint32_t start, s, i = 0;
    UChar32 c;
    uint8_t in_token = 0;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);
        if (u_isUAlphabetic(c) || u_isdigit(c)) {
            if (!in_token) {
                start = s;
                tokens[*tokens_len].start = start;
                tokens[*tokens_len].utflen = 0;

                in_token = 1;
            }

            tokens[*tokens_len].utflen++;
        } else {
            if (in_token) {
                tokens[*tokens_len].len = s - start;
                (*tokens_len)++;
                in_token = 0;
            }
        }
    } while (c > 0);

    return 1;
}

void text_print_ngram(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len) {
    for (uint32_t i = start; i < start + len; i++) {
        token_t token = tokens[i];
        printf("%.*s ", token.len, &text[token.start]);
    }
    printf("\n");
}

uint32_t text_ngram_str_len(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len) {
    uint32_t ngram_len = 0;
    for (uint32_t i = start; i < start + len; i++) {
        ngram_len += tokens[i].len;
    }
    return ngram_len;
}

uint8_t *text_ngram_str(uint8_t *text, token_t *tokens, uint32_t tokens_start,
                        uint32_t tokens_len, uint8_t *str, uint32_t str_len_max) {
    uint32_t k = 0;
    uint32_t tokens_end = tokens_start + tokens_len - 1;
    for (uint32_t i = tokens_start; i <= tokens_end; i++) {
        uint32_t start = tokens[i].start;
        uint32_t end = tokens[i].start + tokens[i].len - 1;
        for (uint32_t j = start; j <= end && k < str_len_max - 1; j++) {
            *(str + k) = text[j];
            k++;
        }
        if (k >= str_len_max - 1) return 0;
        if (i == tokens_end) break;
        *(str + k) = ' ';
        k++;
    }
    *(str + k) = 0;
    return str;
}

uint32_t text_lines(uint8_t *text, token_t *tokens, uint32_t tokens_len,
                    line_t *lines, uint32_t *lines_len) {
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
    return 1;
}

uint32_t text_hash28(uint8_t *text, uint8_t text_len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);
    XXH64_update(&state64, text, text_len);
    return (uint32_t) (XXH64_digest(&state64) & 0xFFFFFFF);
}

uint64_t text_ngram_hash56(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);

    for (uint32_t i = start; i < start + len; i++) {
        token_t token = tokens[i];
        XXH64_update(&state64, text + token.start, token.len);
    }
    return (XXH64_digest(&state64)) >> 8;
}
