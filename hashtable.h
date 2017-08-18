#ifndef TITLE_FINGERPRINT_DB_HASHTABLE_H
#define TITLE_FINGERPRINT_DB_HASHTABLE_H

#include <stdint.h>

#define HASHTABLE_SIZE 16777216
#define ROW_SLOTS_MAX 256
#define MAX_SLOTS_PER_TITLE 5
#define MAX_TITLE_LEN 1024
#define MAX_NAME_LEN 64
#define MAX_LOOKUP_TEXT_LEN 4096

typedef struct stats {
    uint32_t used_hashes;
    uint32_t used_slots;
    uint8_t max_slots;
    uint8_t slots_dist[ROW_SLOTS_MAX + 1];
} stats_t;

// 32 + 30 + 28 + 6
#pragma pack(push, 1)
typedef struct slot {
    uint32_t hash32;
    uint64_t data;
} slot_t;
#pragma pack(pop)

typedef struct row {
    slot_t *slots;
    uint8_t len;
    uint8_t updated;
} row_t;

typedef struct token {
    uint32_t start;
    uint32_t len;
} token_t;

typedef struct line {
    uint32_t start;
    uint32_t len;
} line_t;

typedef struct result {
    uint8_t title[4096];
    uint8_t name[64];
    uint8_t identifiers[4096];
} result_t;

uint32_t init_icu();
uint8_t *process_text(uint8_t *text, uint32_t text_len, token_t *tokens, uint32_t *tokens_len);
void print_ngram(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len);
uint32_t identify(uint8_t *text, result_t *result);
uint32_t index_title(uint8_t *title, uint8_t *name, uint8_t *identifiers);
uint32_t load();
stats_t get_stats();

#endif //TITLE_FINGERPRINT_DB_HASHTABLE_H
