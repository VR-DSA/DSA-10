/* Code to read from a raw data buffer and copy to two output buffers */

#include <time.h>
#include <sys/socket.h>
#include <math.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sched.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <cmath>
#include <string.h>
#include <unistd.h>
#include <netdb.h>

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


void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out, dada_hdu_t * out2, multilog_t * log);
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

void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out, dada_hdu_t * out2, multilog_t * log)
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

  if (dada_hdu_unlock_write (out2) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out2\n");
    }
  dada_hdu_destroy (out2);

  
  
}

void usage()
{
  fprintf (stdout,
	   "dsaX_correlator_copy [options]\n"
	   " -c core   bind process to CPU core\n"
	   " -b blocksize for input/output [def 402653184]\n"
	   " -d blocksize for dump buffer [default 2415919104]\n"
	   " -h print usage\n");
}


int main (int argc, char *argv[]) {

  /* DADA Header plus Data Unit */
  dada_hdu_t* hdu_in = 0;
  dada_hdu_t* hdu_out = 0;
  dada_hdu_t* hdu_out2 = 0;
  
  /* DADA Logger */
  multilog_t* log = 0;
  
  // input data block HDU key
  key_t in_key = 0x0000dada;
  key_t out_key2 = 0x0000eaea;
  key_t out_key = 0x0000caca;

  // command line arguments
  uint64_t blocksize = 402653184;
  uint64_t blocksize2 = 2415919104;
  int core = -1;
  int arg=0;
  

  while ((arg=getopt(argc,argv,"c:b:d:h")) != -1)
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
	case 'b':
	  blocksize = (uint64_t)(atoi(optarg));
	  break;
	case 'd':
	  blocksize2 = (uint64_t)(atoi(optarg));
	  break;
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  // DADA stuff
  
  log = multilog_open ("dsaX_correlator_copy", 0);
  multilog_add (log, stderr);

  // open connection to the in/read DBs
  
  hdu_in  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in, in_key);
  if (dada_hdu_connect (hdu_in) < 0) {
    printf ("dsaX_correlator_copy: could not connect to input buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in) < 0) {
    printf ("dsaX_correlator_copy: could not lock to input buffer\n");
    return EXIT_FAILURE;
  }

  hdu_out  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out, out_key);
  if (dada_hdu_connect (hdu_out) < 0) {
    printf ("dsaX_correlator_copy: could not connect to output  buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out) < 0) {
    dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
    fprintf (stderr, "dsaX_correlator_copy: could not lock to output buffer\n");
    return EXIT_FAILURE;
  }

  hdu_out2  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out2, out_key2);
  if (dada_hdu_connect (hdu_out2) < 0) {
    printf ("dsaX_correlator_copy: could not connect to output2 buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out2) < 0) {
    dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
    fprintf (stderr, "dsaX_correlator_copy: could not lock to output2 buffer\n");
    return EXIT_FAILURE;
  }
  
  // Bind to cpu core
  if (core >= 0)
    {
      printf("binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	printf("dsaX_correlator_copy: failed to bind to core %d\n", core);
    }

  bool observation_complete=0;

  // stuff for copying data
  char * cpbuf = (char *)malloc(sizeof(char)*blocksize);
  int ngulps = (int)(blocksize2/blocksize);
  int gulp = 0;
  char *in_data;
  uint64_t written=0, written2=0;
  uint64_t block_id, bytes_read=0;

  multilog(log, LOG_INFO, "main: have ngulps %d, blocksize %llu, blocksize2 %llu\n",ngulps,blocksize,blocksize2);
    
  // more DADA stuff - deal with headers
  
  uint64_t header_size = 0;

  // read the header from the input HDU
  char * header_in = ipcbuf_get_next_read (hdu_in->header_block, &header_size);
  if (!header_in)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
      return EXIT_FAILURE;
    }

  // now write the output DADA headers
  char * header_out = ipcbuf_get_next_write (hdu_out->header_block);
  if (!header_out)
    {
      multilog(log, LOG_ERR, "could not get next header block [output]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
      return EXIT_FAILURE;
    }
  char * header_out2 = ipcbuf_get_next_write (hdu_out2->header_block);
  if (!header_out2)
    {
      multilog(log, LOG_ERR, "could not get next header2 block [output2]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
      return EXIT_FAILURE;
    }

  // copy the in header to the out header
  memcpy (header_out, header_in, header_size);
  memcpy (header_out2, header_in, header_size);

  // mark the input header as cleared
  if (ipcbuf_mark_cleared (hdu_in->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared [input]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
      return EXIT_FAILURE;
    }

  // mark the output header buffers as filled
  if (ipcbuf_mark_filled (hdu_out->header_block, header_size) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block filled [output]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_filled (hdu_out2->header_block, header_size) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header2 block filled [output2]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
      return EXIT_FAILURE;
    }

  // main reading loop

  multilog(log, LOG_INFO, "main: starting read\n");

  while (!observation_complete) {

    // read a DADA block
    in_data = ipcio_open_block_read (hdu_in->data_block, &bytes_read, &block_id);
    //multilog(log, LOG_INFO, "main: read block which contains %lld bytes\n", bytes_read);

    // copy
    memcpy(cpbuf, in_data, blocksize);

    // write to output 1
    written = ipcio_write (hdu_out->data_block, cpbuf, blocksize);
    if (written < blocksize)
      {
	multilog(log, LOG_INFO, "main: failed to write all data to datablock [output]\n");
	dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
	return EXIT_FAILURE;
      }
    //multilog(log, LOG_INFO, "main: written to main output\n");

    // write to output 2
    written2 = ipcio_write (hdu_out2->data_block, cpbuf, blocksize);
    if (written2 < blocksize)
      {
	multilog(log, LOG_INFO, "main: failed to write all data to datablock2 [output2]\n");
	dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
	return EXIT_FAILURE;
      }
    //multilog(log, LOG_INFO, "main: written to second output\n");
    
    // for exiting
    if (bytes_read < blocksize) {
      observation_complete = 1;
      multilog(log, LOG_INFO, "main: finished, with bytes_read %llu < expected %llu\n", bytes_read, blocksize);
    }

    // close block for reading
    ipcio_close_block_read (hdu_in->data_block, bytes_read);

  }
  

  free(cpbuf);
  dsaX_dbgpu_cleanup (hdu_in, hdu_out, hdu_out2, log);
  
}
  
