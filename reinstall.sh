make uninstall
make clean
libtoolize
aclocal
autoheader -I $JAVA_HOME/include/ -I $JAVA_HOME/include/linux/
automake --add-missing
autoconf
./configure
make
make install
