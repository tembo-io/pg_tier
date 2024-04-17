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

# install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN $HOME/.cargo/bin/rustup default stable

# install pgrx
ARG PGRX_VER=0.11.3
RUN $HOME/.cargo/bin/cargo install cargo-pgrx --version=$PGRX_VER --locked

# init pgrx with postgres
RUN $HOME/.cargo/bin/cargo pgrx init --pg15 $(which pg_config)

# Clone, build and install parquet_s3_fdw
RUN git clone https://github.com/pgspider/parquet_s3_fdw.git && \
    cd parquet_s3_fdw && \
    git checkout v1.1.0 && \
	USE_PGXS=1 make install && \
    cd .. && rm -rf parquet_s3_fdw

# install pg_tier
COPY . .
RUN $HOME/.cargo/bin/cargo pgrx install --pg-config=$(which pg_config)

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
