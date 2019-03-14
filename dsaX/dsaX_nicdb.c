/* Code to receive data from network */

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
#define HDR_SIZE 4096


void dsaX_dbgpu_cleanup (dada_hdu_t * out, multilog_t * log);
uint64_t unpack_hdr(char * packet, char * data);
uint64_t unpack(char * packet, char * data);

uint64_t unpack(char * packet, char * data) {

  uint64_t retval;
  memcpy(&retval, packet, 8);
  memcpy(data, packet+8, PACKET_BLOCK);

  return retval;
  
}

uint64_t unpack_hdr(char * packet, char * data) {

  uint64_t retval;
  memcpy(&retval, packet, 8);
  memcpy(data, packet+8, HDR_SIZE);
  
  return retval;
  
}

void dsaX_dbgpu_cleanup (dada_hdu_t * out, multilog_t * log)
{
  
  if (dada_hdu_unlock_write (out) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out\n");
    }
  dada_hdu_destroy (out);
  
}

void usage()
{
  fprintf (stdout,
	   "dsaX_nicdb [options]\n"
	   " -c core   bind process to CPU core [default 1]\n"
	   " -b blocksize\n"
	   " -i IP to receive from [no default]\n"
	   " -p port to listen on [no default]\n"
	   " -k dada hdu key to write to [default dada]\n"
	   " -h print usage\n");
}

int main (int argc, char *argv[]) {

  /* DADA Header plus Data Unit */
  dada_hdu_t* hdu_out = 0;

  /* DADA Logger */
  multilog_t* log = 0;
  
  // input data block HDU key
  key_t out_key = 0x0000dada;

  // command line arguments
  uint64_t blocksize = 75497472;
  int core = -1;
  char iP[100];
  int portnum;
  int arg=0;

  while ((arg=getopt(argc,argv,"b:c:i:p:k:h")) != -1)
    {
      switch (arg)
	{
	case 'i':
	  strcpy(iP,optarg);
	  break;
	case 'c':
	  core = atoi(optarg);
	  break;
	case 'p':
	  portnum = atoi(optarg);
	  break;
	case 'b':
	  blocksize = (uint64_t)(atoi(optarg));
	  break;
	case 'k':
	  if (sscanf (optarg, "%x", &out_key) != 1) {
	    fprintf (stderr, "dada_db: could not parse key from %s\n", optarg);
	    return EXIT_FAILURE;
	  }
	  break;
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  log = multilog_open ("dsaX_nicdb", 0);
  multilog_add (log, stderr);

  multilog (log, LOG_INFO, "dsaX_nicdb: creating hdu\n");  

  // open connection to the out DB
  
  hdu_out  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out, out_key);
  if (dada_hdu_connect (hdu_out) < 0) {
    printf ("dsaX_nicdb: could not connect to dada buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write (hdu_out) < 0) {
    printf ("dsaX_nicdb: could not lock to dada buffer\n");
    return EXIT_FAILURE;
  }

  printf("binding to core %d\n", core);
  if (core>0) {
    if (dada_bind_thread_to_core(core) < 0)
      printf("dsaX_nicdb: failed to bind to core %d\n", core);
  }


  // set up receive socket
  struct sockaddr_in si_other, si_me;
  int clientSocket, slen=sizeof(si_other);

  if ((clientSocket=socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP))==-1) {
    multilog (log, LOG_ERR, "main: socket error\n");
    dsaX_dbgpu_cleanup (hdu_out, log);
    return EXIT_FAILURE;
  }
  memset((char *) &si_me, 0, sizeof(si_me));
  si_me.sin_family = AF_INET;
  si_me.sin_port = htons(portnum);
  si_me.sin_addr.s_addr = inet_addr(iP);
  bind(clientSocket, (struct sockaddr *)&si_me, sizeof(si_me));
  multilog (log, LOG_INFO, "dsaX_nicdb: connected to send socket\n");

  
  char *packet, *hpacket, *tpacket, *hblock, *dblock;
  packet = (char *)malloc(sizeof(char)*PACKET_SIZE);
  tpacket = (char *)malloc(sizeof(char)*PACKET_SIZE);
  hpacket = (char *)malloc(sizeof(char)*(HDR_SIZE+8));
  hblock = (char *)malloc(sizeof(char)*(HDR_SIZE));
  dblock = (char *)malloc(sizeof(char)*(PACKET_BLOCK));
  uint64_t pctr = 0;

  // deal with header

  // read the header from the input HDU

  //receive header packet
  multilog (log, LOG_INFO, "dsaX_nicdb: waiting for header packet\n");
  recvfrom(clientSocket, hpacket, HDR_SIZE+8, 0,(struct sockaddr *)&si_other,&slen);
  if (unpack_hdr(hpacket, hblock)==0) 
    multilog (log, LOG_INFO, "dsaX_nicdb: received header packet\n");
  else {
    multilog (log, LOG_ERR, "dsaX_nicdb: bad header packet\n");
    dsaX_dbgpu_cleanup (hdu_out, log);
    return EXIT_FAILURE;
  }

  // now write the output DADA header
  char * header_out = ipcbuf_get_next_write (hdu_out->header_block);
  uint64_t header_size = HDR_SIZE;
  if (!header_out)
    {
      multilog(log, LOG_ERR, "could not get next header block [output]\n");
      dsaX_dbgpu_cleanup (hdu_out, log);
      return EXIT_FAILURE;
    }
  // copy the in header to the out header
  memcpy (header_out, hblock, header_size);
  // mark the output header buffer as filled
  if (ipcbuf_mark_filled (hdu_out->header_block, header_size) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block filled [output]\n");
      dsaX_dbgpu_cleanup (hdu_out, log);
      return EXIT_FAILURE;
    }

  // start copying data loop

  int observation_complete=0, rout=1, fullpack;
  uint64_t written=0;
  uint64_t block_ct=0;
  uint64_t block_ct2=0;
  uint64_t block_num=0, pos_block;
  uint64_t npackets = blocksize/PACKET_BLOCK;
  char * out_data, * out_data2;
  out_data = (char *)malloc(sizeof(char)*blocksize);
  out_data2 = (char *)malloc(sizeof(char)*blocksize);
  memset(out_data,0,blocksize);
  memset(out_data2,0,blocksize);
  multilog(log, LOG_INFO, "main: starting observation\n");

  clock_t t1, t2;
  int dropped = 0, totp = 0;
  t1 = clock();
  
  while (!observation_complete) {

    rout=recvfrom(clientSocket, packet, PACKET_SIZE, 0, (struct sockaddr *)&si_other, &slen);
    pctr = unpack(packet, dblock);

    if (pctr==0 || rout==0) {
      multilog (log, LOG_INFO, "dsaX_nicdb: received termination packet\n");
      observation_complete=1;
    }
    else if (rout==PACKET_SIZE) {

      pos_block = (pctr-1)-block_num*npackets;

      // packet is within current block
      if (pos_block<npackets && pos_block>=0) { 
	memcpy(out_data+pos_block*PACKET_BLOCK,dblock,PACKET_BLOCK);
	block_ct++;
      }
      // packet is within next block
      else if (pos_block<2*npackets && pos_block>=npackets) {
	memcpy(out_data2+(pos_block-npackets)*PACKET_BLOCK,dblock,PACKET_BLOCK);
	block_ct2++;
      }
      // packet is in third block
      else if (pos_block>=2*npackets) { 
	
	// write current block to output buffer
	written = ipcio_write (hdu_out->data_block, out_data, blocksize);
	if (written < blocksize)
	  {
	    multilog(log, LOG_INFO, "main: failed to write all data to datablock [output]\n");
	    dsaX_dbgpu_cleanup (hdu_out, log);
	    return EXIT_FAILURE;
	  }
	dropped += (int)(npackets-block_ct);
	totp += (int)(npackets);

	// copy out_data2 to out_data
	memcpy(out_data,out_data2,blocksize);
	memset(out_data2,0,blocksize);
	block_ct = block_ct2;
	block_num++;
	
	// write to next block
	pos_block = (pctr-1)-block_num*npackets;
	if (pos_block<2*npackets && pos_block>=npackets) {
	  memcpy(out_data2+(pos_block-npackets)*PACKET_BLOCK,dblock,PACKET_BLOCK);
	  block_ct2=1;
	}
	
      }
      else {
	multilog(log, LOG_INFO, "main: received packet %g blocks too late\n",(float)(pos_block*1./npackets));
      }
      
    }
    else {
      multilog(log, LOG_INFO, "main: received bad packet length %d, id %llu skipping\n",rout,pctr);
    }

    t2 = clock();
    if (t2>=t1+200000) {
      multilog(log, LOG_INFO, "main: dropped %d of %d packets\n",dropped,totp);
      totp=0;
      dropped=0;
      t1 = clock();
    }
    
    
  }
    
  free(packet);
  free(hpacket);
  free(hblock);
  free(dblock);
  free(tpacket);
  free(out_data);
  free(out_data2);
  close(clientSocket);
  dsaX_dbgpu_cleanup (hdu_out, log);
  
}
