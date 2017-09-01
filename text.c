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
#include <unicode/unorm2.h>

#define XXH_STATIC_LINKING_ONLY

#include "xxhash.h"
#include "text.h"

UNormalizer2 *unorm2;

uint32_t text_init() {
    UErrorCode status = U_ZERO_ERROR;
    unorm2 = unorm2_getNFKDInstance(&status);
    if (status != U_ZERO_ERROR) {
        fprintf(stderr, "unorm2_getNFKDInstance failed, error=%s\n", u_errorName(status));
        return 0;
    }
    return 1;
}

uint32_t text_process(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len,
                      uint32_t *map, uint32_t *map_len, line_t *lines, uint32_t *lines_len) {
    UErrorCode status = U_ZERO_ERROR;
    int32_t max_output_text_len = *output_text_len - 1;
    *output_text_len = 0;
    if(map) *map_len = 0;
    if(lines) *lines_len = 0;
    int32_t output_text_offset = 0;
    UChar uc[16] = {0};

    int32_t si, i = 0;
    UChar32 ci;

    UBool error = 0;
    uint8_t prev_new = 1;

    do {
        if (output_text_offset >= max_output_text_len) {
            error = 1;
            break;
        }

        si = i;

        U8_NEXT(text, i, -1, ci);
        //printf("%C\n", ci);
        if (u_isUAlphabetic(ci)) {

            if(lines) {
                if (prev_new) {
                    lines[*lines_len].start = output_text_offset;
                    (*lines_len)++;
                }
                prev_new = 0;
            }

            int32_t res = unorm2_getDecomposition(unorm2, ci, uc, 16, &status);

            if (res > 0) {
                if (status != U_ZERO_ERROR) {
                    fprintf(stderr, "unorm2_getDecomposition error: %s\n", u_errorName(status));
                    return 0;
                }

                char decomposed_str[16] = {0};
                int32_t decomposed_str_len = 0;

                u_strToUTF8(decomposed_str, 16, &decomposed_str_len, uc, -1, &status);
                if (status != U_ZERO_ERROR) {
                    fprintf(stderr, "u_strToUTF8 error: %s\n", u_errorName(status));
                    return 0;
                }

                int32_t j = 0;
                UChar32 cj;

                do {
                    if (output_text_offset >= max_output_text_len) {
                        error = 1;
                        break;
                    }

                    U8_NEXT(decomposed_str, j, decomposed_str_len, cj);
                    if (u_isUAlphabetic(cj)) {
                        cj = u_tolower(cj);
                        U8_APPEND(output_text, output_text_offset, max_output_text_len, cj, error);
                        if (error) break;

                        if(map) {
                            while (*map_len < output_text_offset) {
                                map[(*map_len)++] = si;
                            }
                        }
                    }
                } while (cj > 0);
                if (error) break;
            } else {
                ci = u_tolower(ci);

                U8_APPEND(output_text, output_text_offset, max_output_text_len, ci, error);
                if (error) break;

                if(map) {
                    while (*map_len < output_text_offset) {
                        map[(*map_len)++] = si;
                    }
                }
            }
        } else if (u_getIntPropertyValue(ci, UCHAR_LINE_BREAK) == U_LB_LINE_FEED) {
            if(lines) {
                if (!prev_new) {
                    lines[(*lines_len) - 1].end = output_text_offset - 1;
                }
                prev_new = 1;
            }
        }
    } while (ci > 0);

    output_text[output_text_offset] = 0;
    *output_text_len = output_text_offset;

    if(lines) {
        if (!prev_new) {
            lines[(*lines_len) - 1].end = *output_text_len - 1;
        }
    }

    return 1;
}

uint32_t text_process_name(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len) {
    UErrorCode status = U_ZERO_ERROR;
    int32_t max_output_text_len = *output_text_len - 1;
    int32_t output_text_offset = 0;
    UChar uc[16] = {0};

    int32_t i = 0;
    UChar32 ci;

    UBool error = 0;
    uint8_t reset = 0;

    do {
        if (output_text_offset >= max_output_text_len) {
            error = 1;
            break;
        }

        U8_NEXT(text, i, -1, ci);
        if (u_isUAlphabetic(ci)) {
            if (reset) {
                output_text_offset = 0;
                reset = 0;
            }
            int32_t res = unorm2_getDecomposition(unorm2, ci, uc, 16, &status);

            if (res > 0) {
                if (status != U_ZERO_ERROR) {
                    fprintf(stderr, "unorm2_getDecomposition error: %s\n", u_errorName(status));
                    return 0;
                }

                char decomposed_str[16] = {0};
                int32_t decomposed_str_len = 0;

                u_strToUTF8(decomposed_str, 16, &decomposed_str_len, uc, -1, &status);
                if (status != U_ZERO_ERROR) {
                    fprintf(stderr, "u_strToUTF8 error: %s\n", u_errorName(status));
                    return 0;
                }

                UChar32 cj;
                int32_t j = 0;

                do {
                    if (output_text_offset >= max_output_text_len) {
                        error = 1;
                        break;
                    }
                    U8_NEXT(decomposed_str, j, decomposed_str_len, cj);
                    if (u_isUAlphabetic(cj)) {
                        cj = u_tolower(cj);
                        U8_APPEND(output_text, output_text_offset, max_output_text_len, cj, error);
                        if (error) break;
                    }
                } while (cj > 0);
                if (error) break;
            } else {
                ci = u_tolower(ci);
                U8_APPEND(output_text, output_text_offset, max_output_text_len, ci, error);
                if (error) break;
            }
        } else {
            reset = 1;
        }
    } while (ci > 0);

    if (error) return 0;

    output_text[output_text_offset] = 0;
    *output_text_len = output_text_offset;

    return 1;
}

uint32_t text_hash28(uint8_t *text, uint32_t text_len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);
    XXH64_update(&state64, text, text_len);
    return (uint32_t) (XXH64_digest(&state64) & 0xFFFFFFF);
}

uint64_t text_hash56(uint8_t *text, uint32_t text_len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);
    XXH64_update(&state64, text, text_len);
    return (XXH64_digest(&state64)) >> 8;
}

uint32_t text_original_str(uint8_t *text, uint32_t *map, uint32_t map_len,
                           uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max) {
    uint32_t original_start = map[start];
    uint32_t original_end = map[end];

    uint8_t *p;
    uint8_t *u = str;

    uint32_t s, i = original_start;
    UChar32 c;

    uint8_t prev_white = 0;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);

        if (u_isWhitespace(c)) {
            if (!prev_white) {
                *u++ = ' ';
            }
            prev_white = 1;
        } else {
            prev_white = 0;
            for (uint32_t j = s; j < i; j++) {
                *u++ = *(text + j);
            }
        }
    } while (c > 0 && i <= original_end);
    *u = 0;
    return 1;
}

uint32_t text_original_name(uint8_t *text, uint32_t *map, uint32_t map_len,
                            uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max) {
    uint32_t original_start = map[start];
    uint32_t original_end = map[end];

    uint8_t *p;
    uint8_t *u = str;

    uint32_t s, i = original_start;
    UChar32 c;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);

        if (!u_isWhitespace(c)) {
            for (uint32_t j = s; j < i; j++) {
                *u++ = *(text + j);
            }
        }
    } while (c > 0 && i <= original_end);
    *u = 0;
    return 1;
}

