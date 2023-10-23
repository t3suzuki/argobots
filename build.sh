./autogen.sh
./configure --prefix=$PWD/install --enable-affinity
# change -O2 to -O3
make && make install
