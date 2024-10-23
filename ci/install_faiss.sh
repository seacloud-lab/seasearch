sudo apt update
sudo apt install -y wget gnupg

sudo wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
| gpg --dearmor |sudo tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null

echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" |sudo tee /etc/apt/sources.list.d/oneAPI.list

sudo apt update
sudo apt install -y intel-oneapi-mkl-devel git

export MKL_PATH=/opt/intel/oneapi/mkl/latest/lib/intel64
sudo apt install -y swig
cd /tmp
git clone https://github.com/facebookresearch/faiss.git
cd faiss
git checkout v1.8.0
cmake -B build -DFAISS_ENABLE_GPU=OFF \
    -DFAISS_ENABLE_C_API=ON \
    -DFAISS_ENABLE_PYTHON=OFF \
    -DBLA_VENDOR=Intel10_64_dyn  \
    -DBUILD_SHARED_LIBS=ON \
    "-DMKL_LIBRARIES=-Wl,--start-group;${MKL_PATH}/libmkl_intel_lp64.a;${MKL_PATH}/libmkl_gnu_thread.a;${MKL_PATH}/libmkl_core.a;-Wl,--end-group" \
    .
make -C build
sudo make -C build install
sudo cp /tmp/faiss/build/c_api/libfaiss_c.so /usr/lib
sudo cp /tmp/faiss/build/faiss/libfaiss.so /usr/lib