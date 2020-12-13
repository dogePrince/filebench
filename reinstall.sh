make uninstall
make clean
rm parser_gram.c parser_gram.h parser_gram.o

libtoolize
aclocal
autoheader -I $JAVA_HOME/include/ -I $JAVA_HOME/include/linux/
automake --add-missing
autoconf
./configure
make
make install
