#!/bin/bash
set -e

USEARCH_VERSION="2.25.2"

mkdir -p ./tmp/usearch
cd ./tmp/usearch

curl -L -O "https://github.com/unum-cloud/USearch/releases/download/v${USEARCH_VERSION}/usearch_linux_amd64_${USEARCH_VERSION}.deb"
sudo apt-get install -y ./usearch_linux_amd64_${USEARCH_VERSION}.deb
sudo ldconfig
