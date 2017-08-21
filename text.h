#ifndef TITLE_FINGERPRINT_DB_TEXT_H
#define TITLE_FINGERPRINT_DB_TEXT_H

typedef struct token {
    uint32_t start;
    uint32_t len;
    uint32_t utflen;
} token_t;

typedef struct line {
    uint32_t start;
    uint32_t len;
} line_t;

uint32_t text_init();

uint8_t *text_transform(uint8_t *text, uint32_t text_len);

int text_tokens(uint8_t *text, token_t *tokens, uint32_t *tokens_len);

void text_print_ngram(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len);

uint32_t text_ngram_str_len(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len);

uint8_t *text_ngram_str(uint8_t *text, token_t *tokens, uint32_t tokens_start,
                        uint32_t tokens_len, uint8_t *str, uint32_t str_len_max);

uint32_t text_lines(uint8_t *text, token_t *tokens, uint32_t tokens_len,
                    line_t *lines, uint32_t *lines_len);

uint32_t text_hash28(uint8_t *text, uint8_t text_len);

uint64_t text_ngram_hash56(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len);

#endif //TITLE_FINGERPRINT_DB_TEXT_H
