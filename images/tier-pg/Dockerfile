ARG PGTIER_VER=0.0.3
ARG PGRX_VER=0.11.3
ARG PG_VERSION=15

FROM quay.io/tembo/tembo-local:latest
USER root

RUN apt-get update \
    && apt-get install -y \
	ca-certificates \
	curl \
	git \
	make \
    automake \
    ccache \
    cmake \
    g++ \
    libboost-all-dev \
    libcurl4-openssl-dev \
    libevent-dev \
    libprotobuf-dev \
    libprotoc-dev \
    libtool \
    libxml2-dev \
    libxml2-utils \
    libxslt-dev \
    protobuf-compiler \
    xsltproc \
    && rm -rf /var/lib/apt/lists/*

# Clone and build AWS SDK for C++
RUN git clone https://github.com/aws/aws-sdk-cpp.git && \
    cd aws-sdk-cpp && \
    git checkout 1.9.263 && \
    git submodule update --init --recursive && \
    mkdir build && cd build && \
    cmake -DBUILD_ONLY="s3;core;config;sts;cognito-identity;transfer;identity-management" -DAUTORUN_UNIT_TESTS=OFF -DCMAKE_CXX_FLAGS=-Wno-error=deprecated-declarations .. && \
    make -j$(nproc) && \
    make install && \
    cd ../../ && rm -rf aws-sdk-cpp

# Clone and build Apache Arrow
RUN git clone https://github.com/apache/arrow.git && \
    cd arrow && \
    git checkout apache-arrow-7.0.1 && \
    cd cpp && \
    mkdir build && cd build && \
    cmake -DARROW_PARQUET=ON -DARROW_S3=ON -DARROW_WITH_SNAPPY=ON .. && \
    make -j$(nproc) && \
    make install && \
    cd ../../ && rm -rf arrow

WORKDIR /

# init pgrx with postgres
RUN $HOME/.cargo/bin/cargo pgrx init --pg15 $(which pg_config)

# Clone, build and install parquet_s3_fdw
RUN git clone https://github.com/pgspider/parquet_s3_fdw.git && \
    cd parquet_s3_fdw && \
    git checkout v1.1.0 && \
	USE_PGXS=1 make install && \
    cd .. && rm -rf parquet_s3_fdw

# install pg_tier
RUN git clone https://github.com/tembo-io/pg_tier.git && \
    cd pg_tier && git fetch origin v$PGTIER_VER && \
    git checkout v$PGTIER_VER && \
    $HOME/.cargo/bin/cargo pgrx install --pg-config=$(which pg_config) && \
    cd .. && rm -rf pg_tier

# cache all extensions
RUN set -eux; \
      cp -r $(pg_config --pkglibdir)/* /tmp/pg_pkglibdir; \
      cp -r $(pg_config --sharedir)/* /tmp/pg_sharedir;

ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
ENV XDG_CACHE_HOME=/var/lib/postgresql/data/tembo/.cache

RUN apt-get update \
    && apt-get install -y ca-certificates

# Revert the postgres user to id 26
RUN usermod -u 26 postgres
USER 26
