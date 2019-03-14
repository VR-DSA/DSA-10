/* Code to send data across network to remote receiver */

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
#include <math.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>

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

#define PACKET_SIZE 8200
#define PACKET_BLOCK 8192

void dsaX_dbgpu_cleanup (dada_hdu_t * in, multilog_t * log);
void packetise (uint64_t ct, char * message, uint64_t plen, char * packet);

void packetise (uint64_t ct, char * message, uint64_t plen, char * packet) {

  memcpy(packet,&ct,8);
  memcpy(packet+8, message, plen);

  
}

void dsaX_dbgpu_cleanup (dada_hdu_t * in, multilog_t * log)
{
  
  if (dada_hdu_unlock_read (in) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in\n");
    }
  dada_hdu_destroy (in);
  
}

void usage()
{
  fprintf (stdout,
	   "dsaX_dbnic [options]\n"
	   " -c core   bind process to CPU core [default 1]\n"
	   " -k key [default dada]\n"
	   " -i remote ip\n"
	   " -p port\n"
	   " -n NWAIT [default 30000]\n"
	   " -h print usage\n");
}

int main (int argc, char *argv[]) {

  /* DADA Header plus Data Unit */
  dada_hdu_t* hdu_in = 0;

  /* DADA Logger */
  multilog_t* log = 0;

  // input data block HDU key
  key_t in_key = 0x0000dada;

  // command line arguments
  int core = -1;
  char * ip = "10.10.10.10";
  int portnum;
  int NWAIT = 30000;
  
  int arg = 0;

  while ((arg=getopt(argc,argv,"c:k:i:p:n:h")) != -1)
    {
      switch (arg)
	{
	case 'c':
	  core = atoi(optarg);
	  break;
	case 'k':
	  if (sscanf (optarg, "%x", &in_key) != 1) {
	    fprintf (stderr, "dada_db: could not parse key from %s\n", optarg);
	    return EXIT_FAILURE;
	  }
	  break;
	case 'i':
	  ip = optarg;
	  break;
	case 'p':
	  portnum = atoi(optarg);
	  break;
	case 'n':
	  NWAIT = atoi(optarg);
	  break;
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  log = multilog_open ("dsaX_dbnic", 0);
  multilog_add (log, stderr);

  
  multilog (log, LOG_INFO, "dsaX_dbnic: creating hdu\n");

  // open connection to the in/read DBs
  
  hdu_in  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in, in_key);
  if (dada_hdu_connect (hdu_in) < 0) {
    printf ("dsaX_dbnic: could not connect to dada buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in) < 0) {
    printf ("dsaX_dbnic: could not lock to dada buffer\n");
    return EXIT_FAILURE;
  }

  printf("binding to core %d\n", core);
  if (core>0) {
    if (dada_bind_thread_to_core(core) < 0)
      printf("dsaX_dbnic: failed to bind to core %d\n", core);
  }

  // set up send socket
  int srvSocket;
  struct sockaddr_in si_other;
  int slen=sizeof(si_other);

  srvSocket=socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  memset((char *) &si_other, 0, sizeof(si_other));
  si_other.sin_family = AF_INET;
  si_other.sin_port = htons(portnum);
  si_other.sin_addr.s_addr = inet_addr(ip);
    
  
  char *packet, *hpacket;
  packet = (char *)malloc(sizeof(char)*PACKET_SIZE);
  uint64_t pctr = 0;
  uint64_t blockctr;
  multilog (log, LOG_INFO, "dsaX_dbnic: ready to transmit\n");

  // deal with header
  
  uint64_t header_size = 0;

  // read the header from the input HDU
  char * header_in = ipcbuf_get_next_read (hdu_in->header_block, &header_size);
  if (!header_in)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in, log);
      return EXIT_FAILURE;
    }

  // send header packet to client
  hpacket = (char *)malloc(sizeof(char)*(8+header_size));
  packetise(pctr, header_in, header_size, hpacket);
  if (sendto(srvSocket,hpacket,header_size+8,0,(struct sockaddr *)&si_other,slen)==-1) {
    multilog(log ,LOG_ERR, "main: failed to send header packet\n");
    dsaX_dbgpu_cleanup (hdu_in, log);
    return EXIT_FAILURE;
  }

  multilog(log ,LOG_INFO, "main: sent header packet\n");
  
  // mark the input header as cleared
  if (ipcbuf_mark_cleared (hdu_in->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared [input]\n");
      dsaX_dbgpu_cleanup (hdu_in, log);
      return EXIT_FAILURE;
    }

  // for sleeping
  struct timespec req, rem;
  req.tv_sec = 0;
  req.tv_nsec = 500L;
  int *tti = malloc(sizeof(int)*NWAIT);
  clock_t t1, t2;
  sleep(1);

  // start copying data loop

  int observation_complete=0, sendo;
  uint64_t blocksize = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in->data_block);
  uint64_t block_id, bytes_read=0;
  uint64_t npackets = blocksize/PACKET_BLOCK;
  char * in_data, tmpstr[9];

  multilog(log, LOG_INFO, "main: starting observation with %llu packets per block\n",npackets);

  while (!observation_complete) {

    //clock_t begin = clock();
    
    // open block
    in_data = ipcio_open_block_read (hdu_in->data_block, &bytes_read, &block_id);

    // send data in block
    blockctr=0;
    while (blockctr<npackets) {

      //t1 = clock();
      pctr++;
      memset(&packet[0], 0, sizeof(packet));
      memcpy(packet,&pctr,8);
      memcpy(packet+8, in_data+PACKET_BLOCK*blockctr, PACKET_BLOCK);
      sendo=sendto(srvSocket,packet,PACKET_SIZE,0,(struct sockaddr *)&si_other,slen);

      //t2 = clock();
      //while (t2-t1<NWAIT) 
	//t2 = clock();
      for (int ti=0;ti<NWAIT;ti++)
	tti[ti]=ti*ti;
      //nanosleep(&req, &rem);
      blockctr++;

    }
    
    // for exiting
    if (bytes_read < blocksize) {
      observation_complete = 1;
      multilog(log, LOG_INFO, "main: finished, with bytes_read %llu < expected %llu\n", bytes_read, blocksize);
    }

    // close block for reading
    ipcio_close_block_read (hdu_in->data_block, bytes_read);
    //multilog(log, LOG_INFO, "main: finished a block\n");

    //clock_t end = clock();
    //multilog(log, LOG_INFO, "main: used %g seconds\n",(double)(end-begin)/CLOCKS_PER_SEC);
    
  }

  memset(&packet[0], 0, sizeof(packet));
  sendto(srvSocket,packet,PACKET_SIZE,0,(struct sockaddr *)&si_other,slen);

  close(srvSocket);
  free(packet);
  free(hpacket);
  free(tti);
  dsaX_dbgpu_cleanup (hdu_in, log);
  
}
