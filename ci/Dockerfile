FROM ubuntu:noble

ENV DEBIAN_FRONTEND=noninteractive
ENV FAISS_VERSION=1.8.0
ENV USEARCH_VERSION=2.25.2

RUN <<EOF
    set -eux
    apt-get update
    apt-get install -y --no-install-recommends \
        curl ca-certificates unzip cmake make pkg-config g++ libopenblas-dev

    cd /tmp
    curl -L -o faiss.zip "https://github.com/facebookresearch/faiss/archive/refs/tags/v${FAISS_VERSION}.zip"
    unzip faiss.zip
    cd ./faiss-${FAISS_VERSION}
    cmake -B build -DBUILD_TESTING=OFF -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_C_API=ON -DBUILD_SHARED_LIBS=ON -DFAISS_ENABLE_PYTHON=OFF .
    make -C build faiss
    make -C build install
    install build/c_api/libfaiss_c.so /usr/local/lib/
    cd /tmp
    rm -rf /tmp/faiss*

    curl -L -o usearch.deb "https://github.com/unum-cloud/USearch/releases/download/v${USEARCH_VERSION}/usearch_linux_amd64_${USEARCH_VERSION}.deb"
    apt-get install -y ./usearch.deb
    rm -f usearch.deb

    ldconfig
EOF
