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


#include "cube/cube.h"
#include "xgpu.h"

#include <thrust/fill.h>
#include <thrust/device_vector.h>
#include <thrust/sequence.h>
#include <thrust/functional.h>
#include <thrust/transform.h>

// if NBLOCKS*NTHREADS is changed, the kernel may need to be changed
#define NBLOCKS 100
#define NTHREADS 32

void dsaX_dbgpu_cleanup (dada_hdu_t * in1, dada_hdu_t * in2, dada_hdu_t * in3, dada_hdu_t * in4, dada_hdu_t * in5, dada_hdu_t * out, multilog_t * log);
int dada_bind_thread_to_core (int core);
void simple_extract(Complex *mat, float *output);

// n is number in each input. typically nsamps_gulp*300
// inputs i1-i5 are SNAPs, in [frequency, time, (ant, pol, ri)] order
// output needs to be [time, frequency, ant, pol, ri]
__global__
void massage(char *i1, char *output, int nsamps_gulp) {

  int n = nsamps_gulp*250*8*5;
  int idx = blockIdx.x*blockDim.x + threadIdx.x; // global index
  int m = n/NBLOCKS/NTHREADS; // number of ints to process per thread

  int sidx, sfq_idx, st_idx, in1, oidx, ti, sn_idx, t1;
  
  for (int i=0;i<m;i++) {

    sn_idx = (int)((idx*m+i) / (250*nsamps_gulp*8)); // snap idx
    t1 = (int)((idx*m+i) % (250*nsamps_gulp*8)); // remainder from multi snaps
    sfq_idx = (int)(t1/8/nsamps_gulp); // fq_idx
    st_idx = (int)((t1/8)  % nsamps_gulp); // t_idx
    sidx = (int)(t1 % 8); // char idx
    oidx = 64*(st_idx*250+sfq_idx)+sidx+8*sn_idx; // output idx at start
    ti = (idx*m+i);

    output[oidx] = i1[ti]/16;
   
    
  }
    

}



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



void usage()
{
  fprintf (stdout,
	   "dsaX_correlator [options]\n"
	   " -c core   bind process to CPU core\n"
	   " -h        print usage\n");
}

int main (int argc, char *argv[]) {
  
  /* DADA Header plus Data Unit */
  dada_hdu_t* hdu_in1 = 0;
  dada_hdu_t* hdu_in2 = 0;
  dada_hdu_t* hdu_in3 = 0;
  dada_hdu_t* hdu_in4 = 0;
  dada_hdu_t* hdu_in5 = 0;
  dada_hdu_t* hdu_out = 0;

  /* DADA Logger */
  multilog_t* log = 0;

  int core = -1;

  // input data block HDU key
  key_t in_key1 = 0x0000bdad;
  key_t in_key2 = 0x0000bdcd;
  key_t in_key3 = 0x0000bddd;
  key_t in_key4 = 0x0000bbbb;
  key_t in_key5 = 0x0000bbab;
  key_t out_key = 0x0000eada;

  int arg = 0;
  int nsamps_gulp=49152;
  int nout=202752000;

  while ((arg=getopt(argc,argv,"c:n:h")) != -1)
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
	case 'n':
	  nout = atoi(optarg);
	  break;
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  
  
  // DADA stuff
  
  log = multilog_open ("dsaX_massager", 0);

  multilog_add (log, stderr);

  multilog (log, LOG_INFO, "dsaX_massager: creating in hdus\n");

  // open connection to the in/read DBs
  
  hdu_in1  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in1, in_key1);
  if (dada_hdu_connect (hdu_in1) < 0) {
    printf ("dsaX_massager: could not connect to dada buffer1\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in1) < 0) {
    printf ("dsaX_massager: could not lock to dada buffer1\n");
    return EXIT_FAILURE;
  }

  hdu_in2  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in2, in_key2);
  if (dada_hdu_connect (hdu_in2) < 0) {
    printf ("dsaX_massager: could not connect to dada buffer2\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in2) < 0) {
    printf ("dsaX_massager: could not lock to dada buffer2\n");
    return EXIT_FAILURE;
  }

  hdu_in3  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in3, in_key3);
  if (dada_hdu_connect (hdu_in3) < 0) {
    printf ("dsaX_massager: could not connect to dada buffer3\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in3) < 0) {
    printf ("dsaX_massager: could not lock to dada buffer3\n");
    return EXIT_FAILURE;
  }

  hdu_in4  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in4, in_key4);
  if (dada_hdu_connect (hdu_in4) < 0) {
    printf ("dsaX_massager: could not connect to dada buffer4\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in4) < 0) {
    printf ("dsaX_massager: could not lock to dada buffer4\n");
    return EXIT_FAILURE;
  }

  hdu_in5  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in5, in_key5);
  if (dada_hdu_connect (hdu_in5) < 0) {
    printf ("dsaX_massager: could not connect to dada buffer5\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in5) < 0) {
    printf ("dsaX_massager: could not lock to dada buffer5\n");
    return EXIT_FAILURE;
  }

  hdu_out  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out, out_key);
  if (dada_hdu_connect (hdu_out) < 0) {
    printf ("dsaX_massager: could not connect to output  buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out) < 0) {
    fprintf (stderr, "dsaX_massager: could not lock to output buffer\n");
    return EXIT_FAILURE;
  }
  
  
  // Bind to cpu core
  if (core >= 0)
    {
      printf("binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	printf("dsaX_correlator: failed to bind to core %d\n", core);
    }

  bool observation_complete=0;
	
  // more DADA stuff - deal with headers
  
  uint64_t header_size = 0;

  // read the headers from the input HDUs and mark as cleared
  char * header_in1 = ipcbuf_get_next_read (hdu_in1->header_block, &header_size);
  if (!header_in1)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_cleared (hdu_in1->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  char * header_in2 = ipcbuf_get_next_read (hdu_in2->header_block, &header_size);
  if (!header_in2)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_cleared (hdu_in2->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  char * header_in3 = ipcbuf_get_next_read (hdu_in3->header_block, &header_size);
  if (!header_in3)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_cleared (hdu_in3->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  char * header_in4 = ipcbuf_get_next_read (hdu_in4->header_block, &header_size);
  if (!header_in4)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_cleared (hdu_in4->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  char * header_in5 = ipcbuf_get_next_read (hdu_in5->header_block, &header_size);
  if (!header_in5)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  if (ipcbuf_mark_cleared (hdu_in5->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }

  // deal with output header
  char * header_out = ipcbuf_get_next_write (hdu_out->header_block);
  if (!header_out)
    {
      multilog(log, LOG_ERR, "could not get next header block [output]\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  memcpy (header_out, header_in1, header_size);
  if (ipcbuf_mark_filled (hdu_out->header_block, header_size) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block filled [output]\n");
      dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
      return EXIT_FAILURE;
    }
  
  
  uint64_t block_size = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in1->data_block);
  uint64_t  bytes_read1 = 0, bytes_read2 = 0, bytes_read3 = 0, bytes_read4 = 0, bytes_read5 = 0, written;
  uint64_t block_out = (uint64_t)(nout);
  uint64_t sgulp, ogulp;
  
  // set up

  // set up xgpu
  XGPUInfo xgpu_info;
  int syncOp = SYNCOP_DUMP;
  int xgpu_error = 0;
  xgpuInfo(&xgpu_info);
  XGPUContext context;
  context.array_h = NULL;
  context.matrix_h = NULL;
  xgpu_error = xgpuInit(&context, 0);
  if(xgpu_error) {
    multilog(log, LOG_ERR, "dsaX_xgpu: xGPU error %d\n", xgpu_error);
    dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
    return EXIT_FAILURE;
  }
  ComplexInput *array_h = context.array_h; // this is pinned memory
  Complex *cuda_matrix_h = context.matrix_h;
  float *output_vis = (float *)malloc(sizeof(float)*2*2*125*55);
  memset((char *)array_h,0,2*context.array_len);
  char *output_buffer = (char *)malloc(sizeof(char)*block_out);

  // host vectors for new data
  thrust::device_vector<char> d_i1(250*nsamps_gulp*8*5);
  thrust::device_vector<char> d_massage(nsamps_gulp * 250 * 16 * 2 * 2);
  uint64_t bytes_in_block = nsamps_gulp * 250 * 16 * 2 * 2;
  thrust::fill(d_massage.begin(),d_massage.end(),0);
  char *omassage = thrust::raw_pointer_cast(d_massage.data());
  uint64_t block_id1, block_id2, block_id3, block_id4, block_id5;
  char *block1, *block2, *block3, *block4, *block5;
  char *i1;
  i1 = thrust::raw_pointer_cast(d_i1.data());
  int nspec =250;

  // register stuff with gpu
  dada_cuda_dbregister(hdu_in1);
  dada_cuda_dbregister(hdu_in2);
  dada_cuda_dbregister(hdu_in3);
  dada_cuda_dbregister(hdu_in4);
  dada_cuda_dbregister(hdu_in5);
  
  // start everything
  
  multilog(log, LOG_INFO, "dsaX_massager: starting observation\n");

  while (!observation_complete) {

    block1 = ipcio_open_block_read (hdu_in1->data_block, &bytes_read1, &block_id1);
    block2 = ipcio_open_block_read (hdu_in2->data_block, &bytes_read2, &block_id2);
    block3 = ipcio_open_block_read (hdu_in3->data_block, &bytes_read3, &block_id3);
    block4 = ipcio_open_block_read (hdu_in4->data_block, &bytes_read4, &block_id4);
    block5 = ipcio_open_block_read (hdu_in5->data_block, &bytes_read5, &block_id5);
      
    // copy blocks to device
    thrust::copy(block1,block1+nspec*nsamps_gulp*8,d_i1.begin());
    thrust::copy(block2,block2+nspec*nsamps_gulp*8,d_i1.begin()+nspec*nsamps_gulp*8);
    thrust::copy(block3,block3+nspec*nsamps_gulp*8,d_i1.begin()+2*nspec*nsamps_gulp*8);
    thrust::copy(block4,block4+nspec*nsamps_gulp*8,d_i1.begin()+3*nspec*nsamps_gulp*8);
    thrust::copy(block5,block5+nspec*nsamps_gulp*8,d_i1.begin()+4*nspec*nsamps_gulp*8);

   
    // massage
    massage<<<NBLOCKS, NTHREADS>>>(i1,omassage,nsamps_gulp);
    cudaDeviceSynchronize();
    
    // loop over sub-samples of input
    sgulp = 0;
    ogulp = 0;
    while (sgulp < bytes_in_block) {

      // select sub-gulp
      thrust::copy(d_massage.begin()+sgulp,d_massage.begin()+sgulp+128*250*64,(char *)array_h);
      
      // run xGPU
      xgpu_error = xgpuCudaXengine(&context, syncOp);
      if(xgpu_error) {
	multilog(log, LOG_ERR, "dsaX_xgpu: xGPU error %d\n", xgpu_error);
	dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
	return EXIT_FAILURE;
      }
      
      // reorder and extract output matrix
      simple_extract(cuda_matrix_h,output_vis);
      
      // copy to output buffer
      //memcpy(output_buffer+ogulp,(char *)output_vis,2*2*250*55*4);
      //memcpy(output_buffer+ogulp,(char *)cuda_matrix_h,136*250*4*2*4);
      // write to output
      written = ipcio_write (hdu_out->data_block, (char *)output_vis, 2*2*125*55*4);
      if (written < 2*2*125*55*4)
	{
	  multilog(log, LOG_INFO, "main: failed to write all data to datablock [output]\n");
	  dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);
	  return EXIT_FAILURE;
	}
      xgpuClearDeviceIntegrationBuffer(&context);

      // increment
      sgulp += 128*250*64;
      //ogulp += 2*2*250*55*4;
      //ogulp += 136*250*4*2*4;
       
    }

    multilog(log, LOG_INFO, "main: written block\n");
    
    
    if (bytes_read1 < block_size || bytes_read2 < block_size || bytes_read3 < block_size || bytes_read4 < block_size || bytes_read5 < block_size)
      observation_complete = 1;
    ipcio_close_block_read (hdu_in1->data_block, bytes_read1);
    ipcio_close_block_read (hdu_in2->data_block, bytes_read2);
    ipcio_close_block_read (hdu_in3->data_block, bytes_read3);
    ipcio_close_block_read (hdu_in4->data_block, bytes_read4);
    ipcio_close_block_read (hdu_in5->data_block, bytes_read5);

  }

  // unregister and free
  dada_cuda_dbunregister(hdu_in1);
  dada_cuda_dbunregister(hdu_in2);
  dada_cuda_dbunregister(hdu_in3);
  dada_cuda_dbunregister(hdu_in4);
  dada_cuda_dbunregister(hdu_in5);
  dsaX_dbgpu_cleanup (hdu_in1, hdu_in2, hdu_in3, hdu_in4, hdu_in5, hdu_out, log);

}


// assumes TRIANGULAR_ORDER for mat (f, baseline, pol, ri)
void simple_extract(Complex *mat, float *output) {

  int in_idx, out_idx;
  for (int bctr=0;bctr<55;bctr++) {
    for (int pol1=0;pol1<2;pol1++) {

      for (int f=0;f<125;f++) {

	out_idx = 2*((bctr*125+f)*2+pol1);
	in_idx = (2*f*136+bctr)*4+pol1*3;
	output[out_idx] = 0.5*(mat[in_idx].real + mat[in_idx+544].real);
	output[out_idx+1] = 0.5*(mat[in_idx].imag + mat[in_idx+544].imag);
	
      }
    }
  }

}


void dsaX_dbgpu_cleanup (dada_hdu_t * in1, dada_hdu_t * in2, dada_hdu_t * in3, dada_hdu_t * in4, dada_hdu_t * in5, dada_hdu_t * out, multilog_t * log)
{
  
  if (dada_hdu_unlock_read (in1) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in1\n");
    }
  dada_hdu_destroy (in1);

  if (dada_hdu_unlock_read (in2) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in2\n");
    }
  dada_hdu_destroy (in2);
  if (dada_hdu_unlock_read (in3) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in3\n");
    }
  dada_hdu_destroy (in3);
  if (dada_hdu_unlock_read (in4) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in4\n");
    }
  dada_hdu_destroy (in4);
  if (dada_hdu_unlock_read (in5) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in5\n");
    }
  dada_hdu_destroy (in5);

  if (dada_hdu_unlock_write (out) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock write on hdu_out\n");
    }
  dada_hdu_destroy (out);
  
}
