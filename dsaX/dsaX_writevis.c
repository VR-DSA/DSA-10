/* This works pretty much like the trigger code. receives a control UDP message 
to store some data for a fixed amount of time.
Message format: length(s)-NAME
Will ignore messages until data recording is over
*/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <time.h>
#include <arpa/inet.h>
#include <sys/syscall.h>

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
#include "dsaX_correlator_udpdb_thread.h"
#include "fitsio.h"

#define CONTROL_PORT 11224

/* global variables */
int quit_threads = 0;
int dump_pending = 0;
int trignum = 0;
int dumpnum = 0;
char iP[100];
char srcnam[1024];
float reclen;

float summed_vis[27500];

void dsaX_dbgpu_cleanup (dada_hdu_t * in, multilog_t * log);

void usage()
{
  fprintf (stdout,
	   "dsaX_image [options]\n"
	   " -c core   bind process to CPU core\n"
	   " -f filename base [default test.fits]\n"
	   " -o freq of chan 1 [default 1530.0]\n"
	   " -i IP to listen to [no default]\n"
	   " -h        print usage\n");
}

void dsaX_dbgpu_cleanup (dada_hdu_t * in, multilog_t * log) {

  if (dada_hdu_unlock_read (in) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in\n");
    }
  dada_hdu_destroy (in);

}

// Thread to control the dumping of data

void control_thread (void * arg) {

  udpdb_t * ctx = (udpdb_t *) arg;
  multilog(ctx->log, LOG_INFO, "control_thread: starting\n");

  // port on which to listen for control commands
  int port = ctx->control_port;

  // buffer for incoming command strings, and setup of socket
  int bufsize = 1024;
  char* buffer = (char *) malloc (sizeof(char) * bufsize);
  memset(buffer, '\0', bufsize);
  const char* whitespace = " ";
  char * command = 0;
  char * args = 0;

  struct addrinfo hints;
  struct addrinfo* res=0;
  memset(&hints,0,sizeof(hints));
  struct sockaddr_storage src_addr;
  socklen_t src_addr_len=sizeof(src_addr);
  hints.ai_family=AF_INET;
  hints.ai_socktype=SOCK_DGRAM;
  getaddrinfo(iP,"11224",&hints,&res);
  int fd;
  ssize_t ct;
  char tmpstr;
  char cmpstr = 'p';
  char *endptr;
  float tmp_reclen;
  
  multilog(ctx->log, LOG_INFO, "control_thread: created socket on port %d\n", port);
  
  while (!quit_threads) {
    
    fd = socket(res->ai_family,res->ai_socktype,res->ai_protocol);
    bind(fd,res->ai_addr,res->ai_addrlen);
    memset(buffer,'\0',sizeof(buffer));
    multilog(ctx->log, LOG_INFO, "control_thread: waiting for packet\n");
    ct = recvfrom(fd,buffer,1024,0,(struct sockaddr*)&src_addr,&src_addr_len);
    
    multilog(ctx->log, LOG_INFO, "control_thread: received buffer string %s\n",buffer);
    trignum++;

    // interpret buffer string
    char * rest = buffer;
    tmp_reclen = (float)(strtof(strtok(rest, "-"),&endptr));
    char *tmp_srcnam = strtok(rest, "-");
    
    if (!dump_pending) {
      reclen = tmp_reclen;
      strcpy(srcnam,tmp_srcnam);
      multilog(ctx->log, LOG_INFO, "control_thread: received command to dump %f s for SRC %s\n",reclen,srcnam);
    }
	
    if (dump_pending)
      multilog(ctx->log, LOG_ERR, "control_thread: BACKED UP - CANNOT dump %f s for SRC %s\n",tmp_reclen,tmp_srcnam);
  
    if (!dump_pending) dump_pending = 1;
    
    close(fd);
    
  }

  free (buffer);

  if (ctx->verbose)
    multilog(ctx->log, LOG_INFO, "control_thread: exiting\n");

  /* return 0 */
  int thread_result = 0;
  pthread_exit((void *) &thread_result);

}

int main (int argc, char *argv[]) {

  /* DADA defs */
  dada_hdu_t* hdu_in = 0;
  multilog_t* log = 0;
  key_t in_key = 0x0000eada;

  /* port for control commands */
  int control_port = CONTROL_PORT;

  /* actual struct with info */
  udpdb_t udpdb;
  
  // command line
  int arg = 0;
  int core = -1;
  int nsamps = 384;
  float fch1 = 1530.0;
  int nchans = 125;
  int npts = 55*125*2*2;
  int nsamps_gulp = 384;
  char fnam[300], foutnam[400];
  sprintf(fnam,"/mnt/nfs/data/alltest");
  
  while ((arg=getopt(argc,argv,"c:f:o:i:h")) != -1)
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
	case 'f':
	  strcpy(fnam,optarg);
	  break;
	case 'o':
	  fch1 = atof(optarg);
	  break;
	case 'i':
	  strcpy(iP,optarg);
	  break;
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  // DADA stuff
  
  log = multilog_open ("dsaX_writevis", 0);
  multilog_add (log, stderr);

  udpdb.log = log;
  udpdb.verbose = 1;
  udpdb.control_port = control_port;

  multilog (log, LOG_INFO, "dsaX_writevis: creating hdu\n");

  hdu_in  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in, in_key);
  if (dada_hdu_connect (hdu_in) < 0) {
    printf ("dsaX_writevis: could not connect to dada buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in) < 0) {
    printf ("dsaX_writevis: could not lock to dada buffer\n");
    return EXIT_FAILURE;
  }

  // Bind to cpu core
  if (core >= 0)
    {
      printf("binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	printf("dsaX_writevis: failed to bind to core %d\n", core);
    }

  int observation_complete=0;

  // more DADA stuff - deal with headers
  
  uint64_t header_size = 0;

  // read the headers from the input HDUs and mark as cleared
  char * header_in = ipcbuf_get_next_read (hdu_in->header_block, &header_size);
  if (!header_in)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_cleared (hdu_in->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared\n");
      dsaX_dbgpu_cleanup (hdu_in, log);
      return EXIT_FAILURE;
    }


  // start control thread
  int rval = 0;
  pthread_t control_thread_id;
  multilog(log, LOG_INFO, "starting control_thread()\n");
  rval = pthread_create (&control_thread_id, 0, (void *) control_thread, (void *) &udpdb);
  if (rval != 0) {
    multilog(log, LOG_INFO, "Error creating control_thread: %s\n", strerror(rval));
    return -1;
  }

  // set up
  FILE *ftime;
  int fctr = 0, integration = 0;
  char tstamp[100];
  double mjd;
  fitsfile *fptr;
  int rownum = 1;
  int fwrite = 0;
  int status=0;
  float mytsamp = nsamps_gulp*8.192e-6*128.;
  int NINTS;
  
  // data stuff
  uint64_t block_size = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in->data_block);
  uint64_t bytes_read = 0, block_id;
  char *block;
  float *data;
  
  // start things

  multilog(log, LOG_INFO, "dsaX_writevis: starting observation\n");

  while (!observation_complete) {

    // read block
    block = ipcio_open_block_read (hdu_in->data_block, &bytes_read, &block_id);
    data = (float *)block; // order is [384 time, 55 baseline, 125 freq, 2 pol, 2 ri]

    // sum input visibilities
    for (int i=0;i<npts;i++) summed_vis[i] = 0.;
    for (int i=0;i<nsamps_gulp;i++) {
      for (int j=0;j<npts;j++)
	summed_vis[j] += data[i*npts+j];
    }

    // get start time, and convert others to samples
    ftime=fopen("/mnt/nfs/runtime/UTC_START.txt","r");
    fscanf(ftime,"%lf\n",&mjd);
    fscanf(ftime,"%[^\n]",&tstamp[0]);
    fclose(ftime);
    
    // check for dump_pending
    if (dump_pending) {

      // if file writing hasn't started
      if (fwrite==0) {

	multilog(log, LOG_INFO, "dsaX_writevis: beginning file write for SRC %s for %f s\n",srcnam,reclen);

	NINTS = (int)(floor(reclen/mytsamp));
	sprintf(foutnam,"%s_%s_%s_%d.fits",fnam,tstamp,srcnam,fctr);
	multilog(log, LOG_INFO, "main: opening new file %s\n",foutnam);
	rownum=1;
	
	char *ttype[] = {"VIS"};
	char *tform[] = {"27500E"}; // assumes classic npts
	char *tunit[] = {"\0"};
	char *antennas = "3-7-2-10-1-4-5-8-6-9";
	char *wsrcnam = srcnam;
	
	char extname[] = "DATA";
	fits_create_file(&fptr, foutnam, &status);
	if (status) multilog(log, LOG_ERR, "create_file FITS error %d\n",status);
	fits_create_tbl(fptr, BINARY_TBL, 0, 1, ttype, tform, tunit, extname, &status);
	if (status) multilog(log, LOG_ERR, "create_tbl FITS error %d\n",status);
	fits_write_key(fptr, TDOUBLE, "MJD", &mjd, "Start MJD", &status);
	fits_write_key(fptr, TFLOAT, "TSAMP", &mytsamp, "Sample time (s)", &status);
	fits_write_key(fptr, TFLOAT, "FCH1", &fch1, "Frequency (MHz)", &status);
	fits_write_key(fptr, TINT, "NCHAN", &nchans, "Channels", &status);
	fits_write_key(fptr, TSTRING, "Antennas", &antennas[0], "Antennas", &status);
	fits_write_key(fptr, TSTRING, "Source", &wsrcnam[0], "Source", &status);	  
	if (status) multilog(log, LOG_ERR, "fits_write FITS error %d\n",status);
	fits_close_file(fptr, &status);

	fwrite=1;
	
      }

      // write data to file
      fits_open_table(&fptr, foutnam, READWRITE, &status);
      fits_write_col(fptr, TFLOAT, 1, rownum, 1, 66000, summed_vis, &status);
      
      rownum += 1;
      fits_update_key(fptr, TINT, "NAXIS2", &rownum, "", &status);
      fits_close_file(fptr, &status);
      integration++;
      if (status) multilog(log, LOG_ERR, "fits_write FITS error %d\n",status);	
      // check if file writing is done
      if (integration==NINTS) {
	integration=0;
	multilog(log, LOG_INFO, "dsaX_writevis: completed file %d\n",fctr);
	fctr++;
	fwrite=0;
	dump_pending=0;
      }
	
    }

    // update mjd
    mjd += nsamps_gulp*128.*8.192e-6/86400.;
            
    // close off loop
    if (bytes_read < block_size)
      observation_complete = 1;

    ipcio_close_block_read (hdu_in->data_block, bytes_read);
    
  }

  // close control thread
  multilog(log, LOG_INFO, "joining control_thread\n");
  quit_threads = 1;
  void* result=0;
  pthread_join (control_thread_id, &result);
  
  dsaX_dbgpu_cleanup(hdu_in, log);
 
}
