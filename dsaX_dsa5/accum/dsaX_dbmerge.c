/* Will merge up to 10 dbs into out db, assuming ACCUM data */

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


// BINDS PROCESS TO PARTICULAR CPU CORE
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

// UNLOCKS CONNECTION TO DADA HEADER DATA UNIT
void dsaX_dbgpu_cleanup_in (dada_hdu_t * in, multilog_t * log);
void dsaX_dbgpu_cleanup_out (dada_hdu_t * out, multilog_t * log);

void dsaX_dbgpu_cleanup_in (dada_hdu_t * in, multilog_t * log)
{
  
  if (dada_hdu_unlock_read (in) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in\n");
    }
  dada_hdu_destroy (in);
}

void dsaX_dbgpu_cleanup_out (dada_hdu_t * out, multilog_t * log)
{
  if (dada_hdu_unlock_write (out) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out\n");
    }
  dada_hdu_destroy (out); 
}

// WILL APPEAR WITH -h OPTION
void usage()
{
  fprintf (stdout,
	   "dsaX_dbmerge [options]\n"
	   " -c core   bind process to CPU core\n"
	   " -db0-9    keys of dbs to merge\n"
	   " -o        output db\n"
	   " -h print usage\n");
}

// MAIN PART OF PROGRAM
int main (int argc, char *argv[]) {

  /* DADA Logger */
  multilog_t* log = 0;
  
  // command line arguments
  key_t in_key0, in_key1, in_key2, in_key3, in_key4, in_key5, in_key6, in_key7, in_key8, in_key9, out_key; 
  int NDB = 0;
  int core=-1;
  for (int i=1;i<argc;i++) {

    if (strcmp(argv[i],"-c")==0) {
      core=atoi(argv[i+1]);
    }

    if (strcmp(argv[i],"-h")==0) {
      usage();
      return EXIT_SUCCESS;
    }

    if (strcmp(argv[i],"-o")==0) {
      if (sscanf (argv[i+1], "%x", &out_key) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
    }

    if (strcmp(argv[i],"-db0")==0) {
      if (sscanf (argv[i+1], "%x", &in_key0) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }

    if (strcmp(argv[i],"-db1")==0) {
      if (sscanf (argv[i+1], "%x", &in_key1) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }

    if (strcmp(argv[i],"-db2")==0) {
      if (sscanf (argv[i+1], "%x", &in_key2) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }

    if (strcmp(argv[i],"-db3")==0) {
      if (sscanf (argv[i+1], "%x", &in_key3) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }

    if (strcmp(argv[i],"-db2=4")==0) {
      if (sscanf (argv[i+1], "%x", &in_key4) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }

    if (strcmp(argv[i],"-db5")==0) {
      if (sscanf (argv[i+1], "%x", &in_key5) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }

    if (strcmp(argv[i],"-db6")==0) {
      if (sscanf (argv[i+1], "%x", &in_key6) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }
    
    if (strcmp(argv[i],"-db7")==0) {
      if (sscanf (argv[i+1], "%x", &in_key7) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }
      
    if (strcmp(argv[i],"-db8")==0) {
      if (sscanf (argv[i+1], "%x", &in_key8) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }

    if (strcmp(argv[i],"-db9")==0) {
      if (sscanf (argv[i+1], "%x", &in_key9) != 1) {
	fprintf (stderr, "dada_db: could not parse key from %s\n", argv[i+1]);
	return EXIT_FAILURE;
      }
      NDB++;
    }
    
  }

  log = multilog_open ("dsaX_dbmerge", 0);
  multilog_add (log, stderr);

  // OPEN CONNECTION TO IN DBs

  dada_hdu_t* hdu_in0 = 0;
  dada_hdu_t* hdu_in1 = 0;
  dada_hdu_t* hdu_in2 = 0;
  dada_hdu_t* hdu_in3 = 0;
  dada_hdu_t* hdu_in4 = 0;
  dada_hdu_t* hdu_in5 = 0;
  dada_hdu_t* hdu_in6 = 0;
  dada_hdu_t* hdu_in7 = 0;
  dada_hdu_t* hdu_in8 = 0;
  dada_hdu_t* hdu_in9 = 0;
  dada_hdu_t* hdu_out = 0;
  
  if (NDB>0) {
    hdu_in0 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in0, in_key0);
    if (dada_hdu_connect (hdu_in0) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 0\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in0) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 0\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>1) {
    hdu_in1 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in1, in_key1);
    if (dada_hdu_connect (hdu_in1) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 1\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in1) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 1\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>2) {
    hdu_in2 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in2, in_key2);
    if (dada_hdu_connect (hdu_in2) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 2\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in2) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 2\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>3) {
    hdu_in3 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in3, in_key3);
    if (dada_hdu_connect (hdu_in3) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 3\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in3) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 3\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>4) {
    hdu_in4 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in4, in_key4);
    if (dada_hdu_connect (hdu_in4) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 4\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in4) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 4\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>5) {
    hdu_in5 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in5, in_key5);
    if (dada_hdu_connect (hdu_in5) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 5\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in5) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 5\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>6) {
    hdu_in6 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in6, in_key6);
    if (dada_hdu_connect (hdu_in6) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 6\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in6) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 6\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>7) {
    hdu_in7 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in7, in_key7);
    if (dada_hdu_connect (hdu_in7) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 7\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in7) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 7\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>8) {
    hdu_in8 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in8, in_key8);
    if (dada_hdu_connect (hdu_in8) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 8\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in8) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 8\n");
      return EXIT_FAILURE;
    }
  }

  if (NDB>9) {
    hdu_in9 = dada_hdu_create (log);
    dada_hdu_set_key (hdu_in9, in_key9);
    if (dada_hdu_connect (hdu_in9) < 0) {
      printf ("dsaX_dbmerge: could not connect to input buffer 9\n");
      return EXIT_FAILURE;
    }
    if (dada_hdu_lock_read (hdu_in9) < 0) {
      printf ("dsaX_dbmerge: could not lock to input buffer 9\n");
      return EXIT_FAILURE;
    }
  }

  // open connection to the out DB

  hdu_out  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out, out_key);
  if (dada_hdu_connect (hdu_out) < 0) {
    printf ("dsaX_dbmerge: could not connect to dada buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write (hdu_out) < 0) {
    printf ("dsaX_dbmerge: could not lock to dada buffer\n");
    return EXIT_FAILURE;
  }

  // Bind to cpu core
  if (core >= 0)
    {
      printf("binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	printf("dsaX_dbmerge: failed to bind to core %d\n", core);
    }

  bool observation_complete=0;

  // read headers and clear them
  uint64_t header_size = 0;
  char *header_in;
  
  if (NDB>0) {
    char * header_in = ipcbuf_get_next_read (hdu_in0->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in0->header_block);
  }
  if (NDB>1) {
    char * header_in = ipcbuf_get_next_read (hdu_in1->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in1->header_block);
  }
  if (NDB>2) {
    char * header_in = ipcbuf_get_next_read (hdu_in2->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in2->header_block);
  }
  if (NDB>3) {
    char * header_in = ipcbuf_get_next_read (hdu_in3->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in3->header_block);
  }
  if (NDB>4) {
    char * header_in = ipcbuf_get_next_read (hdu_in4->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in4->header_block);
  }
  if (NDB>5) {
    char * header_in = ipcbuf_get_next_read (hdu_in5->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in5->header_block);
  }
  if (NDB>6) {
    char * header_in = ipcbuf_get_next_read (hdu_in6->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in6->header_block);
  }
  if (NDB>7) {
    char * header_in = ipcbuf_get_next_read (hdu_in7->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in7->header_block);
  }
  if (NDB>8) {
    char * header_in = ipcbuf_get_next_read (hdu_in8->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in8->header_block);
  }
  if (NDB>9) {
    char * header_in = ipcbuf_get_next_read (hdu_in9->header_block, &header_size);
    ipcbuf_mark_cleared (hdu_in9->header_block);
  }

  // write to output header
  char * header_out = ipcbuf_get_next_write (hdu_out->header_block);
  memcpy (header_out, header_in, header_size);
  ipcbuf_mark_filled (hdu_out->header_block, header_size);

  // main reading loop
  
  // other reading variables
  uint64_t  bytes_read = 0; // how many bytes were actually read
  char *block; // pointer to dada block
  uint64_t block_id; // block id
  uint64_t block_ptr;
  uint64_t written = 0;
  
  // assumes in0 exists!
  uint64_t blocksize = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in0->data_block);
  unsigned short * out_data, *in_data;
  out_data = (unsigned short *)malloc(sizeof(unsigned short)*blocksize/2);

  multilog(log, LOG_INFO, "main: starting read\n");

  while (!observation_complete) {

    memset(out_data,0,blocksize);
    
    // read blocks and add to output
    
    if (NDB>0) {
      block = ipcio_open_block_read (hdu_in0->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in0->data_block, bytes_read);
    }

    if (NDB>1) {
      block = ipcio_open_block_read (hdu_in1->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in1->data_block, bytes_read);
    }

    if (NDB>2) {
      block = ipcio_open_block_read (hdu_in2->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in2->data_block, bytes_read);
    }

    if (NDB>3) {
      block = ipcio_open_block_read (hdu_in3->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in3->data_block, bytes_read);
    }

    if (NDB>4) {
      block = ipcio_open_block_read (hdu_in4->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in4->data_block, bytes_read);
    }

    if (NDB>5) {
      block = ipcio_open_block_read (hdu_in5->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in5->data_block, bytes_read);
    }

    if (NDB>6) {
      block = ipcio_open_block_read (hdu_in6->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in6->data_block, bytes_read);
    }

    if (NDB>7) {
      block = ipcio_open_block_read (hdu_in7->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in7->data_block, bytes_read);
    }

    if (NDB>8) {
      block = ipcio_open_block_read (hdu_in8->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in8->data_block, bytes_read);
    }

    if (NDB>9) {
      block = ipcio_open_block_read (hdu_in9->data_block, &bytes_read, &block_id);
      in_data = (unsigned short *)(block);
      for (int i=0;i<blocksize/2;i++)
	out_data[i] += in_data[i];

      ipcio_close_block_read (hdu_in9->data_block, bytes_read);
    }

    // write data to DADA buffer
    written = ipcio_write (hdu_out->data_block, (char *)(out_data), blocksize);
    if (written < blocksize)
      {
	multilog(log, LOG_INFO, "main: failed to write all data to datablock [output]\n");
	return EXIT_FAILURE;
      }
    
  }
  
  if (NDB>0) dsaX_dbgpu_cleanup_in (hdu_in0, log);
  if (NDB>1) dsaX_dbgpu_cleanup_in (hdu_in1, log);
  if (NDB>2) dsaX_dbgpu_cleanup_in (hdu_in2, log);
  if (NDB>3) dsaX_dbgpu_cleanup_in (hdu_in3, log);
  if (NDB>4) dsaX_dbgpu_cleanup_in (hdu_in4, log);
  if (NDB>5) dsaX_dbgpu_cleanup_in (hdu_in5, log);
  if (NDB>6) dsaX_dbgpu_cleanup_in (hdu_in6, log);
  if (NDB>7) dsaX_dbgpu_cleanup_in (hdu_in7, log);
  if (NDB>8) dsaX_dbgpu_cleanup_in (hdu_in8, log);
  if (NDB>9) dsaX_dbgpu_cleanup_in (hdu_in9, log);

  dsaX_dbgpu_cleanup_out (hdu_out, log);
  
  free(out_data);
  
}


  
