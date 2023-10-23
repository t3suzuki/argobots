./autogen.sh
./configure --prefix=$PWD/install --enable-affinity --enable-fast=O3 --enable-default-stacksize=1048576
make && make install
