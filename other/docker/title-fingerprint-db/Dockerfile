FROM debian:stretch

RUN apt-get update \
    && apt-get -y install build-essential cmake git autoconf libtool wget \

    && cd /tmp \
    && git clone https://github.com/davidmoreno/onion \
    && cd onion \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make \
    && make install \

    && cd /tmp \
    && wget -O jansson.tar.gz https://github.com/akheron/jansson/archive/v2.10.tar.gz \
    && mkdir jansson \
    && tar -zxf jansson.tar.gz -C jansson --strip-components=1 \
    && cd jansson \
    && autoreconf -i \
    && ./configure \
    && make \
    && make install \

    && cd /tmp \
    && wget -O sqlite.tar.gz http://sqlite.org/2017/sqlite-autoconf-3200000.tar.gz \
    && mkdir sqlite \
    && tar -zxf sqlite.tar.gz -C sqlite --strip-components=1 \
    && cd sqlite \
    && ./configure \
    && make \
    && make install \

    && cd /tmp \
    && wget -O icu.tgz http://download.icu-project.org/files/icu4c/59.1/icu4c-59_1-src.tgz \
    && mkdir icu \
    && tar -zxf icu.tgz -C icu --strip-components=1 \
    && cd icu/source \
    && ./configure \
    && make \
    && make install \

    && cd /tmp \
    && wget -O jemalloc.tar.bz2 https://github.com/jemalloc/jemalloc/releases/download/5.0.1/jemalloc-5.0.1.tar.bz2 \
    && mkdir jemalloc \
    && tar -jxf jemalloc.tar.bz2 -C jemalloc --strip-components=1 \
    && cd jemalloc \
    && ./configure \
    && make \
    && make install \

    && ldconfig

RUN cd /tmp \
    && git clone https://github.com/mrtcode/title-fingerprint-db \
    && cd title-fingerprint-db \
    && mkdir release \
    && cd release \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make \
    && mkdir -p /data/bin \
    && mkdir -p /data/db \
    && cp title-fingerprint-db /data/bin/ \
    && cp -r ../static /data/bin/

WORKDIR /data/bin

ENTRYPOINT ["/data/bin/title-fingerprint-db", "-d", "/data/db", "-p", "80"]
