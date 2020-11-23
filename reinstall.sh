make uninstall
make clean
rm parser_gram.c parser_gram.h parser_gram.o

libtoolize
aclocal
autoheader
automake --add-missing
autoconf
./configure
make
make install
