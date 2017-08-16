#ifndef TITLE_FINGERPRINT_DB_DB_H
#define TITLE_FINGERPRINT_DB_DB_H

int db_init(char *directory);
int db_close();
int db_init_hashtable();
int db_init_identifiers();
int db_save_identifiers();
int db_insert_identifier(uint32_t meta_id, uint8_t *identifier, uint32_t identifier_len);
int db_get_identifiers(uint32_t id, uint8_t *dis, uint32_t dis_max_len);

int save_hashtable(row_t *rows, uint32_t rows_len);
int load_hashtable(row_t *rows);

#endif //TITLE_FINGERPRINT_DB_DB_H
