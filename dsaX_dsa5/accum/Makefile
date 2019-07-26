BINDIR=/usr/local/dsaX/bin

CC=gcc
CFLAGS = -g -O2 -pthread -I/usr/local/psrdada/src

CDEPS=dsaX_correlator_def.h dsaX_correlator_udpdb_thread.h
SDEPS=dsaX_spectrometer_def.h dsaX_spectrometer_udpdb_thread.h
COBJ = dsaX_correlator_udpdb_thread.o
SOBJ = dsaX_spectrometer_udpdb_thread.o
LIBS = -L/usr/local/lib -lpsrdada

dsaX_spectrometer_udpdb_thread.o: dsaX_spectrometer_udpdb_thread.c $(SDEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

dsaX_spectrometer_udpdb_thread: $(SOBJ)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)

dsaX_filflag: dsaX_filflag.cu
	/usr/local/cuda-10.1/bin/nvcc -o $@ $^ -ccbin=g++ -I/usr/local/sigproc -I/usr/local/thrust/thrust -I/usr/local/psrdada/src -I/usr/local/include -L/usr/local/lib -lcfitsio -lpsrdada -lsigproc  -O3

dsaX_dbmerge: dsaX_dbmerge.c
	g++ -o $@ $^ -I/usr/local/psrdada/src -I/usr/local/include -L/usr/local/lib -lpsrdada -lm -pthread -g -O2 -L/usr/lib/gcc/x86_64-linux-gnu/5 -lgfortran

.PHONY: clean

clean:
	rm -f *.o *.*~ dsaX_spectrometer_udpdb_thread dsaX_filflag dsaX_dbmerge

install:
	cp dsaX_spectrometer_udpdb_thread dsaX_filflag dsaX_dbmerge  $(BINDIR)

