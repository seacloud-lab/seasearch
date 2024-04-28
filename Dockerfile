# build Seasearch
FROM node:18.16.0-slim as webBuilder
WORKDIR /web
COPY ./web /web/

RUN npm config set registry https://registry.npmmirror.com
RUN npm install
RUN npm run build

############################
# STEP 2 build executable binary
############################
# FROM golang:alpine AS builder
FROM golang:latest as builder
ARG VERSION
ARG COMMIT_HASH
ARG BUILD_DATE

# build faiss
RUN sed -i 's#http://deb.debian.org/#http://mirrors.tuna.tsinghua.edu.cn/#g' /etc/apt/sources.list.d/debian.sources   \
    && wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
    | gpg --dearmor | tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null \
    && echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" | tee /etc/apt/sources.list.d/oneAPI.list \
    && apt update \
    && apt install -y gcc cmake \
    && apt install -y intel-oneapi-mkl-devel \
    && export MKL_PATH=/opt/intel/oneapi/mkl/latest/lib/intel64 \
    && apt install -y swig\
    && cd /tmp \
    && git clone https://github.com/facebookresearch/faiss.git \
    && cd faiss \
    && cmake -B build -DFAISS_ENABLE_GPU=OFF \
    -DFAISS_ENABLE_C_API=ON \
    -DFAISS_ENABLE_PYTHON=OFF \
    -DBLA_VENDOR=Intel10_64_dyn  \
    -DBUILD_SHARED_LIBS=ON \
    "-DMKL_LIBRARIES=-Wl,--start-group;${MKL_PATH}/libmkl_intel_lp64.a;${MKL_PATH}/libmkl_gnu_thread.a;${MKL_PATH}/libmkl_core.a;-Wl,--end-group" \
    . \
    && make -C build \
    && make -C build install \
    && cp /tmp/faiss/build/c_api/libfaiss_c.so /usr/lib \
    && cp /tmp/faiss/build/faiss/libfaiss.so /usr/lib

RUN update-ca-certificates
# RUN apk update && apk add --no-cache git
# Create zincsearch user.
ENV USER=zincsearch
ENV GROUP=zincsearch
ENV UID=10001
ENV GID=10001
# See https://stackoverflow.com/a/55757473/12429735RUN
RUN groupadd --gid "${GID}" "${GROUP}"
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    --gid "${GID}" \
    "${USER}"
# Create default directories for persistent ZincSearch data used in final build stage.
# It follows the Linux filesystem hierarchy pattern
# https://tldp.org/LDP/Linux-Filesystem-Hierarchy/html/var.html
RUN mkdir -p /var/lib/zincsearch /data && chown zincsearch:zincsearch /var/lib/zincsearch /data
WORKDIR $GOPATH/src/github.com/zincsearch/zincsearch/
COPY . .

COPY --from=webBuilder /web/dist web/dist

# Fetch dependencies.
# Using go get.
RUN go mod tidy

RUN go build -o seasearch ./cmd/zincsearch/main.go

# clean up go packages
RUN rm -rf /go/pkg/*

EXPOSE 4080
# Run the zincsearch binary.
ENTRYPOINT ["/go/src/github.com/zincsearch/zincsearch/seasearch"]