FROM ubuntu:noble

ENV DEBIAN_FRONTEND=noninteractive
ENV FAISS_VERSION=1.8.0
ENV USEARCH_VERSION=2.25.2

COPY ci/build.sh /usr/local/bin/build.sh
RUN bash /usr/local/bin/build.sh
