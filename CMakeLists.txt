#cmake_minimum_required(VERSION 3.8)
project(title-fingerprint-db)

set(CMAKE_C_STANDARD 99)

set(SOURCE_FILES main.c ht.c db.c xxhash.c text.c)
add_executable(title-fingerprint-db ${SOURCE_FILES})

set(CMAKE_C_FLAGS_RELEASE "-O2")

target_link_libraries(title-fingerprint-db icuio icui18n icuuc icudata onion sqlite3 jansson pthread jemalloc)