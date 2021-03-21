FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y \
        clang-11 \
        cmake \
        git \
        expect \
        libboost1.71-all-dev \
        libhwloc-dev \
        libncurses5-dev \
        libpq-dev \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        lld \
        lsb-release \
        man \
        parallel \
        postgresql-server-dev-all \
        software-properties-common \
        sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true

COPY test_pipeline.sh /test_pipeline.sh
ENTRYPOINT ["/test_pipeline.sh"]