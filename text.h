#ifndef TITLE_FINGERPRINT_DB_TEXT_H
#define TITLE_FINGERPRINT_DB_TEXT_H

typedef struct token {
    uint32_t start;
    uint32_t len;
    uint32_t utflen;
} token_t;

typedef struct line {
    uint32_t start;
    uint32_t end;
} line_t;

uint32_t text_init();

uint32_t text_process(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len,
                   uint32_t *map, uint32_t *map_len, line_t *lines, uint32_t *lines_len);

uint32_t text_process_name(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len);

uint32_t text_hash28(uint8_t *text, uint32_t text_len);

uint64_t text_hash56(uint8_t *text, uint32_t text_len);

uint32_t text_original_str(uint8_t *text, uint32_t *map, uint32_t map_len,
                           uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max);

uint32_t text_original_name(uint8_t *text, uint32_t *map, uint32_t map_len,
                            uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max);



#endif //TITLE_FINGERPRINT_DB_TEXT_H
