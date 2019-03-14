// -*- c++ -*-
#include <iostream>
#include <algorithm>
using std::cout;
using std::cerr;
using std::endl;
#include <stdio.h>
#include <stdlib.h>
#include <cmath>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <time.h>

#include "sock.h"
#include "tmutil.h"
#include "dada_client.h"
#include "dada_def.h"
#include "dada_hdu.h"
#include "multilog.h"
#include "ipcio.h"
#include "ipcbuf.h"
#include "dada_affinity.h"
#include "ascii_header.h"

#include "fitsio.h"

#include <thrust/fill.h>
#include <thrust/device_vector.h>
#include <thrust/host_vector.h>
#include <thrust/sequence.h>
#include <thrust/functional.h>
#include <thrust/transform.h>

void transpose(int *dst, const int *src, size_t n, size_t p);

void transpose(int *dst, const int *src, size_t n, size_t p)  {
    size_t block = 32;
    for (size_t i = 0; i < n; i += block) {
        for(size_t j = 0; j < p; ++j) {
            for(size_t b = 0; b < block && i + b < n; ++b) {
                dst[j*n + i + b] = src[(i + b)*p + j];
            }
        }
    }
}

void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out1, dada_hdu_t * out2, dada_hdu_t * out3, dada_hdu_t * out4, dada_hdu_t * out5, dada_hdu_t * out6, multilog_t * log);
int dada_bind_thread_to_core (int core);

int dada_bind_thread_to_core(int core)
{

  cpu_set_t set;
  pid_t tpid;

  CPU_ZERO(&set);
    CPU_SET(core, &set);
      tpid = syscall(SYS_gettid);

  if (sched_setaffinity(tpid, sizeof(cpu_set_t), &set) < 0) {
    printf("failed to set cpu affinity: %s", strerror(errno));
    return -1;
  }
  
  CPU_ZERO(&set);
  if ( sched_getaffinity(tpid, sizeof(cpu_set_t), &set) < 0 ) {
    printf("failed to get cpu affinity: %s", strerror(errno));
    return -1;
  }

  return 0;
}

void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out1, dada_hdu_t * out2, dada_hdu_t * out3, dada_hdu_t * out4, dada_hdu_t * out5, multilog_t * log)
{
  
  if (dada_hdu_unlock_read (in) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in\n");
    }
  dada_hdu_destroy (in);

  if (dada_hdu_unlock_write (out1) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out1\n");
    }
  dada_hdu_destroy (out1);

  if (dada_hdu_unlock_write (out2) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out2\n");
    }
  dada_hdu_destroy (out2);

  if (dada_hdu_unlock_write (out3) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out3\n");
    }
  dada_hdu_destroy (out3);

  if (dada_hdu_unlock_write (out4) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out4\n");
    }
  dada_hdu_destroy (out4);

  if (dada_hdu_unlock_write (out5) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out5\n");
    }
  dada_hdu_destroy (out5);
  
}

void usage()
{
  fprintf (stdout,
	   "dsaX_beamformer_fanout [options]\n"
	   " -c core   bind process to CPU core\n"
	   " -n OUT_NSAMPS [default 98304]\n"
	   " -h print usage\n");
}

int main (int argc, char *argv[]) {
  
  /* DADA Header plus Data Unit */
  dada_hdu_t* hdu_in = 0;
  dada_hdu_t* hdu_out1 = 0;
  dada_hdu_t* hdu_out2 = 0;
  dada_hdu_t* hdu_out3 = 0;
  dada_hdu_t* hdu_out4 = 0;
  dada_hdu_t* hdu_out5 = 0;
  
  /* DADA Logger */
  multilog_t* log = 0;
  
  // data block HDU keys
  key_t in_key = 0x0000caca;
  key_t out_key1 = 0x0000dbda;
  key_t out_key2 = 0x0000dcda;
  key_t out_key3 = 0x0000ddda;
  key_t out_key4 = 0x0000ebda;
  key_t out_key5 = 0x0000ecda;

  // command line arguments
  int core = -1;
  int arg=0;
  int OUT_NSAMPS = 98304;
  
  while ((arg=getopt(argc,argv,"c:n:h")) != -1)
    {
      switch (arg)
	{
	case 'c':
	  core = atoi(optarg);
	  break;
	case 'n':
	  OUT_NSAMPS = atoi(optarg);
	  break;
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  // DADA stuff
  
  log = multilog_open ("dsaX_correlator_fanout", 0);
  multilog_add (log, stderr);

  // open connection to the in/read DBs
  
  hdu_in  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in, in_key);
  if (dada_hdu_connect (hdu_in) < 0) {
    printf ("dsaX_correlator_fanout: could not connect to input buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in) < 0) {
    printf ("dsaX_correlator_fanout: could not lock to input buffer\n");
    return EXIT_FAILURE;
  }

  hdu_out1  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out1, out_key1);
  if (dada_hdu_connect (hdu_out1) < 0) {
    printf ("dsaX_correlator_fanout: could not connect to output buffer1\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out1) < 0) {
    dsaX_dbgpu_cleanup (hdu_in, hdu_out1, hdu_out2, hdu_out3, hdu_out4, hdu_out5, log);
    fprintf (stderr, "dsaX_correlatir_fanout: could not lock to output buffer1\n");
    return EXIT_FAILURE;
  }

  hdu_out2  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out2, out_key2);
  if (dada_hdu_connect (hdu_out2) < 0) {
    printf ("dsaX_correlator_fanout: could not connect to output buffer2\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out2) < 0) {
    dsaX_dbgpu_cleanup (hdu_in, hdu_out1, hdu_out2, hdu_out3, hdu_out4, hdu_out5, log);
    fprintf (stderr, "dsaX_correlator_fanout: could not lock to output buffer2\n");
    return EXIT_FAILURE;
  }

  hdu_out3  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out3, out_key3);
  if (dada_hdu_connect (hdu_out3) < 0) {
    printf ("dsaX_correlator_fanout: could not connect to output buffer3\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out3) < 0) {
    dsaX_dbgpu_cleanup (hdu_in, hdu_out1, hdu_out2, hdu_out3, hdu_out4, hdu_out5, log);
    fprintf (stderr, "dsaX_correlator_fanout: could not lock to output buffer3\n");
    return EXIT_FAILURE;
  }

  hdu_out4  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out4, out_key4);
  if (dada_hdu_connect (hdu_out4) < 0) {
    printf ("dsaX_correlator_fanout: could not connect to output buffer4\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out4) < 0) {
    dsaX_dbgpu_cleanup (hdu_in, hdu_out1, hdu_out2, hdu_out3, hdu_out4, hdu_out5, log);
    fprintf (stderr, "dsaX_correlator_fanout: could not lock to output buffer4\n");
    return EXIT_FAILURE;
  }

  hdu_out5  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out5, out_key5);
  if (dada_hdu_connect (hdu_out5) < 0) {
    printf ("dsaX_correlator_fanout: could not connect to output buffer5\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out5) < 0) {
    dsaX_dbgpu_cleanup (hdu_in, hdu_out1, hdu_out2, hdu_out3, hdu_out4, hdu_out5, log);
    fprintf (stderr, "dsaX_correlator_fanout: could not lock to output buffer5\n");
    return EXIT_FAILURE;
  }
  
  // Bind to cpu core
  if (core >= 0)
    {
      printf("binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	printf("dsaX_correlator_fanout: failed to bind to core %d\n", core);
    }

  // more DADA stuff - deal with headers
  
  uint64_t header_size = 0;

  // read the header from the input HDU, and get output header blocks
  char * header_in = ipcbuf_get_next_read (hdu_in->header_block, &header_size);
  char * header_out1 = ipcbuf_get_next_write (hdu_out1->header_block);
  char * header_out2 = ipcbuf_get_next_write (hdu_out2->header_block);
  char * header_out3 = ipcbuf_get_next_write (hdu_out3->header_block);
  char * header_out4 = ipcbuf_get_next_write (hdu_out4->header_block);
  char * header_out5 = ipcbuf_get_next_write (hdu_out5->header_block);
 
  // copy the in header to the out header
  memcpy (header_out1, header_in, header_size);
  memcpy (header_out2, header_in, header_size);
  memcpy (header_out3, header_in, header_size);
  memcpy (header_out4, header_in, header_size);
  memcpy (header_out5, header_in, header_size);

  // mark the header buffers as cleared/filled
  ipcbuf_mark_cleared (hdu_in->header_block);
  ipcbuf_mark_filled (hdu_out1->header_block, header_size);
  ipcbuf_mark_filled (hdu_out2->header_block, header_size);
  ipcbuf_mark_filled (hdu_out3->header_block, header_size);
  ipcbuf_mark_filled (hdu_out4->header_block, header_size);
  ipcbuf_mark_filled (hdu_out5->header_block, header_size);
  
  // main reading loop

  multilog(log, LOG_INFO, "main: starting read\n");
  bool observation_complete=0;
  bool started_recording=0;
  int nints=0;
  
  // variables
  char * h_data;
  int *h_outdata, *h_intdata, *iter;
  h_outdata = (int *)malloc(sizeof(int)*OUT_NSAMPS*300);
  iter = (int *)malloc(sizeof(int)*OUT_NSAMPS*2048);
  uint64_t block_size = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in->data_block);
  uint64_t  bytes_read = 0;
  uint64_t block_id;
  int bytes_to_write = OUT_NSAMPS*250*2*2;
  uint64_t written=0;

  while (!observation_complete) {

    // read a DADA block
    h_data = ipcio_open_block_read (hdu_in->data_block, &bytes_read, &block_id);
    h_intdata = (int *)h_data;
    transpose(iter,h_intdata,OUT_NSAMPS,2048);
    
    // do the writing
    thrust::copy(iter+350*OUT_NSAMPS,iter+600*OUT_NSAMPS,h_outdata);
    written = ipcio_write (hdu_out1->data_block, (char *) h_outdata, bytes_to_write);
    thrust::copy(iter+600*OUT_NSAMPS,iter+850*OUT_NSAMPS,h_outdata);
    written = ipcio_write (hdu_out2->data_block, (char *) h_outdata, bytes_to_write);
    thrust::copy(iter+850*OUT_NSAMPS,iter+1100*OUT_NSAMPS,h_outdata);
    written = ipcio_write (hdu_out3->data_block, (char *) h_outdata, bytes_to_write);
    thrust::copy(iter+1100*OUT_NSAMPS,iter+1350*OUT_NSAMPS,h_outdata);
    written = ipcio_write (hdu_out4->data_block, (char *) h_outdata, bytes_to_write);
    thrust::copy(iter+1350*OUT_NSAMPS,iter+1600*OUT_NSAMPS,h_outdata);
    written = ipcio_write (hdu_out5->data_block, (char *) h_outdata, bytes_to_write);

    // close block for reading
    ipcio_close_block_read (hdu_in->data_block, bytes_read);
    multilog(log, LOG_INFO, "main: finished a block\n");

    if (bytes_read < block_size) {
      observation_complete = 1;
      multilog(log, LOG_INFO, "main: finished, with bytes_read %llu < expected %llu\n", bytes_read, block_size);
      break;
    }

  }

  dsaX_dbgpu_cleanup (hdu_in, hdu_out1, hdu_out2, hdu_out3, hdu_out4, hdu_out5, log);
  free(h_outdata);
  free(iter);
  
}

