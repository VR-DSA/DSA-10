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
#include <omp.h>

#include "dada_cuda.h"
#include "dada_client.h"
#include "dada_def.h"
#include "dada_hdu.h"
#include "multilog.h"
#include "ipcio.h"
#include "ipcbuf.h"
#include "dada_affinity.h"
#include "ascii_header.h"


#include <thrust/fill.h>
#include <thrust/device_vector.h>
#include <thrust/sequence.h>
#include <thrust/functional.h>
#include <thrust/transform.h>

void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out, multilog_t * log);
int dada_bind_thread_to_core (int core);
void massage (char * in, char * out, uint64_t block_size);

void massage (char * in, char * out, uint64_t block_size) {

  for (uint64_t i=0;i<block_size;i++) {

    out[2*i] = (char)(((unsigned char)(in[i]) & (unsigned char)(15)) << 4);
    out[2*i+1] = (char)((unsigned char)(in[i]) & (unsigned char)(240));

  }

}



int main (int argc, char *argv[]) {
  
  /* DADA Header plus Data Unit */
  dada_hdu_t* hdu_in = 0;
  dada_hdu_t* hdu_out = 0;

  /* DADA Logger */
  multilog_t* log = 0;

  int core = -1;

  // input data block HDU key
  key_t in_key = 0x0000eada;
  key_t out_key = 0x0000fada;
  int arg = 0;
  
  while ((arg=getopt(argc,argv,"c:k:o:h")) != -1)
    {
      switch (arg)
	{
	case 'c':
	  if (optarg)
	    {
	      core = atoi(optarg);
	      break;
	    }
	  else
	    {
	      printf ("ERROR: -c flag requires argument\n");
	      return EXIT_FAILURE;
	    }
	case 'k':
	  sscanf(optarg, "%x", &in_key);
	  break;
	case 'o':
	  sscanf(optarg, "%x", &out_key);
	  break;
	case 'h':
	  return EXIT_SUCCESS;
	}
    }
  
  // DADA stuff
  
  log = multilog_open ("dsaX_single", 0);

  multilog_add (log, stderr);

  multilog (log, LOG_INFO, "dsaX_single: creating hdus\n");

  hdu_in  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in, in_key);
  if (dada_hdu_connect (hdu_in) < 0) {
    printf ("dsaX: could not connect to dada buffer in\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in) < 0) {
    printf ("dsaX: could not lock to dada buffer inb\n");
    return EXIT_FAILURE;
  }

  hdu_out  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out, out_key);
  if (dada_hdu_connect (hdu_out) < 0) {
    printf ("dsaX: could not connect to output  buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out) < 0) {
    fprintf (stderr, "dsaX: could not lock to output buffer\n");
    return EXIT_FAILURE;
  }

  // Bind to cpu core
  if (core >= 0)
    {
      printf("binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	printf("dsaX: failed to bind to core %d\n", core);
    }

  bool observation_complete=0;

  uint64_t header_size = 0;

  // deal with headers
  char * header_in = ipcbuf_get_next_read (hdu_in->header_block, &header_size);
  if (!header_in)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_cleared (hdu_in->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }

  char * header_out = ipcbuf_get_next_write (hdu_out->header_block);
  if (!header_out)
    {
      multilog(log, LOG_ERR, "could not get next header block [output]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }
  memcpy (header_out, header_in, header_size);
  if (ipcbuf_mark_filled (hdu_out->header_block, header_size) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block filled [output]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }
  
  uint64_t block_size = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in->data_block);
  uint64_t block_out = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_out->data_block);
  multilog(log, LOG_INFO, "main: have input and output block sizes %d %d\n",block_size,block_out);
  uint64_t  bytes_read = 0;
  
  char * block, * output_buffer;
  output_buffer = (char *)malloc(sizeof(char)*block_out);
  uint64_t written, block_id;

  // set up
  
  multilog(log, LOG_INFO, "dsaX_single: starting observation\n");

  while (!observation_complete) {

    // open block
    block = ipcio_open_block_read (hdu_in->data_block, &bytes_read, &block_id);

    // do the massage
    massage(block, output_buffer, block_size);
    
    // write to output
    written = ipcio_write (hdu_out->data_block, output_buffer, block_out);
    if (written < block_out)
      {
	multilog(log, LOG_INFO, "main: failed to write all data to datablock [output]\n");
	dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
	return EXIT_FAILURE;
      }

    //multilog(log, LOG_INFO, "main: written block\n");

    if (bytes_read < block_size)
      observation_complete = 1;

    ipcio_close_block_read (hdu_in->data_block, bytes_read);

  }


  free(output_buffer);
  dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
  
}


void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out, multilog_t * log)
{

  if (dada_hdu_unlock_read (in) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in\n");
    }
  dada_hdu_destroy (in);

  if (dada_hdu_unlock_write (out) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out\n");
    }
  dada_hdu_destroy (out);

}
