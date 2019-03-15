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

#include "fitsio.h"
#include <src/sigproc.h>
#include <src/header.h>

#define NCHAN 2048

// binning for single-point flagging
#define BF 16
#define BT 25

FILE *output;

void send_string(char *string) /* includefile */
{
  int len;
  len=strlen(string);
  fwrite(&len, sizeof(int), 1, output);
  fwrite(string, sizeof(char), len, output);
}

void send_float(char *name,float floating_point) /* includefile */
{
  send_string(name);
  fwrite(&floating_point,sizeof(float),1,output);
}

void send_double (char *name, double double_precision) /* includefile */
{
  send_string(name);
  fwrite(&double_precision,sizeof(double),1,output);
}

void send_int(char *name, int integer) /* includefile */
{
  send_string(name);
  fwrite(&integer,sizeof(int),1,output);
}

void send_char(char *name, char integer) /* includefile */
{
  send_string(name);
  fwrite(&integer,sizeof(char),1,output);
}


void send_long(char *name, long integer) /* includefile */
{
  send_string(name);
  fwrite(&integer,sizeof(long),1,output);
}

void send_coords(double raj, double dej, double az, double za) /*includefile*/
{
  if ((raj != 0.0) || (raj != -1.0)) send_double("src_raj",raj);
  if ((dej != 0.0) || (dej != -1.0)) send_double("src_dej",dej);
  if ((az != 0.0)  || (az != -1.0))  send_double("az_start",az);
  if ((za != 0.0)  || (za != -1.0))  send_double("za_start",za);
}

void dsaX_dbgpu_cleanup (dada_hdu_t * hdu_in, dada_hdu_t * hdu_out, multilog_t * log);
int dada_bind_thread_to_core (int core);

// functor to do the scaling
__device__ float *s1, *s2, *s3;

struct da_functor
{

  __device__
  int operator()(const int x, const int y) const {

    int i = (int)(y % (NCHAN)); 

    if (i>1888) return __float2int_rn(64.0);
    else
      return __float2int_rn(x*s1[i]/s2[i]+s3[i]);
    
  }
};
int dada_bind_thread_to_core(int core)
{

  cpu_set_t set;
  pid_t tpid;

  CPU_ZERO(&set);
    CPU_SET(core, &set);
      tpid = syscall(SYS_gettid);

  if (sched_setaffinity(tpid, sizeof(cpu_set_t), &set) < 0) {
      fprintf(stderr, "failed to set cpu affinity: %s", strerror(errno));
          return -1;
	    }

  CPU_ZERO(&set);
    if ( sched_getaffinity(tpid, sizeof(cpu_set_t), &set) < 0 ) {
        fprintf(stderr, "failed to get cpu affinity: %s", strerror(errno));
	    return -1;
	      }

  return 0;
}

void usage()
{
  fprintf (stdout,
	   "dsaX_filflag [options]\n"
	   " -c core   bind process to CPU core\n"
	   " -f val    flagging level (0-nothing, 1-bandpassing, 2-birdies, 3-time-series, 4-brights)\n"
	   " -w        write filterbank file\n"
	   " -n name   file name base [default slog]\n"
	   " -k dada_in\n"
	   " -l dada_out\n"
	   " -h        print usage\n");
}

int main (int argc, char *argv[]) {

  cudaSetDevice(0);
  
  /* DADA Header plus Data Unit */
  dada_hdu_t* hdu_in = 0;
  dada_hdu_t* hdu_out = 0;

  /* DADA Logger */
  multilog_t* log = 0;

  int core = -1;

  // input data block HDU key
  key_t in_key = 0x0000dada;

  // output data block HDU key
  key_t out_key = 0x0000eada;

  // command line
  int arg = 0;
  char fnam[200];
  sprintf(fnam,"slog");
  int filty=0;
  int flaglev=0;
  
  while ((arg=getopt(argc,argv,"c:k:l:f:n:wh")) != -1)
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
	      fprintf (stderr, "ERROR: -c flag requires argument\n");
	      return EXIT_FAILURE;
	    }
	case 'k':
	  if (sscanf (optarg, "%x", &in_key) != 1) {
	    fprintf (stderr, "dada_db: could not parse key from %s\n", optarg);
	    return EXIT_FAILURE;
	  }
	  break;
	case 'l':
	  if (sscanf (optarg, "%x", &out_key) != 1) {
	    fprintf (stderr, "dada_db: could not parse key from %s\n", optarg);
	    return EXIT_FAILURE;
	  }
	  break;
	case 'f':
	  flaglev=atoi(optarg);
	  if (flaglev<0 || flaglev>5) {
	    fprintf (stderr, "bad flaglev %s\n", optarg);
	    return EXIT_FAILURE;
	  }
	  break;
	case 'w':
	  filty=1;
	  break;
	case 'n':
	  strcpy(fnam,optarg);
	  break;
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  // DADA stuff

  log = multilog_open ("dsaX_filflag", 0);
  multilog_add (log, stderr);
  
  multilog (log, LOG_INFO, "dsaX_filflag: creating in hdu\n");
  // open connection to the in/read DB
  hdu_in  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in, in_key);
  if (dada_hdu_connect (hdu_in) < 0) {
    fprintf (stderr, "dsaX_spectrometer_reorder: could not connect to dada buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in) < 0) {
    fprintf (stderr, "dsaX_spectrometer_reorder: could not lock to dada buffer\n");
    return EXIT_FAILURE;
  }

  // open connection to the out/write DB
  hdu_out = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out, out_key);
  if (dada_hdu_connect (hdu_out) < 0)
    {
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      fprintf (stderr, "dsaX_spectrometer_reorder: could not connect to eada buffer\n");
      return EXIT_FAILURE;
    }
  if (dada_hdu_lock_write(hdu_out) < 0)
    {
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      fprintf (stderr, "dsaX_spectrometer_reorder: could not lock4 to eada buffer\n");
      return EXIT_FAILURE;
    }

  if (core >= 0)
    {
      fprintf(stderr, "binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	fprintf(stderr, "dsaX_spectrometer_reorder: failed to bind to core %d\n", core);
    }

  bool observation_complete=0;

  // more DADA stuff
  
  uint64_t header_size = 0;

  // read the header from the input HDU
  char * header_in = ipcbuf_get_next_read (hdu_in->header_block, &header_size);
  if (!header_in)
    {
      multilog(log ,LOG_ERR, "main: could not read next header\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }

  // now write the output DADA header
  char * header_out = ipcbuf_get_next_write (hdu_out->header_block);
  if (!header_out)
    {
      multilog(log, LOG_ERR, "could not get next header block [output]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }

  
  // copy the in header to the out header
  memcpy (header_out, header_in, header_size);

  // mark the input header as cleared
  if (ipcbuf_mark_cleared (hdu_in->header_block) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block cleared [input]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }

  // mark the output header buffer as filled
  if (ipcbuf_mark_filled (hdu_out->header_block, header_size) < 0)
    {
      multilog (log, LOG_ERR, "could not mark header block filled [output]\n");
      dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
      return EXIT_FAILURE;
    }
  
  
  // setup vars
  uint64_t BLOCKSIZE = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in->data_block);
  // ASSUME THIS IS FOR BOTH INPUT AND OUTPUT
  uint64_t NSAMPS_GULP = BLOCKSIZE/NCHAN/2;
  uint64_t bytes_to_read;
  uint64_t block_id;
  char *   block;
  int bytes_to_write = BLOCKSIZE;
  uint64_t written=0;
  int ibyte, idx, idxo, idxs, idxg;
  
  // allocate memory to output
  unsigned short * out_data, * outout_data;
  thrust::host_vector<int> h_gulpdata(2048*NSAMPS_GULP);
  thrust::device_vector<int> d_gulpdata(2048*NSAMPS_GULP);
  thrust::device_vector<int> d_idx(2048*NSAMPS_GULP);
  thrust::sequence(d_idx.begin(),d_idx.end());
  out_data = (unsigned short *)malloc(sizeof(unsigned short)*BLOCKSIZE/2);
  outout_data = (unsigned short *)malloc(sizeof(unsigned short)*BLOCKSIZE/2);
  char *h_indata;
  uint64_t  bytes_read = 0, clipped, clipped_ts;
  int bp[2048], old_bp[2048], ts[NSAMPS_GULP];
  int ts_sum, ds_sum;
  // 11.8 is stddev of chi square with mean 64, 1 dof
  int thresh_ts = (int)((2048*64*BT)+(3.5*sqrt(2048.*BT*11.8)));
  float thresh_diff = (0.5/sqrt(NSAMPS_GULP*1.));
  float thresh_rms = 3.0;
  unsigned short repval = (unsigned short)(64);
  int clipthresh = (int)(64.*BT*BF+6.*sqrt(11.8*BT*BF));
  int clipthresh_ss = (int)(64.+6.*sqrt(11.8));
  unsigned short tmp;
  int intct = 0;
  int started_recording = 0;
  char cmd[200];
  uint64_t specnum = 0;
  int nints=0;
  float rmss[2048], tmpval;

  // to scale data
  thrust::host_vector<float> mult(NCHAN), sc(NCHAN), mlt(NCHAN);
  thrust::host_vector<float> mmult(NCHAN), msc(NCHAN), mmlt(NCHAN);
  thrust::device_vector<float> d_mult(NCHAN), d_sc(NCHAN), d_mlt(NCHAN);
  float *s_mult, *s_mlt, *s_sc;
  s_mult = thrust::raw_pointer_cast(&d_mult[0]);
  s_mlt = thrust::raw_pointer_cast(&d_mlt[0]);
  s_sc = thrust::raw_pointer_cast(&d_sc[0]);
  float bpscl = NSAMPS_GULP*64.;

  for (int i=0;i<NCHAN;i++) {
    sc[i] = 0.;
    mult[i] = bpscl;
    if (i<160) mult[i]=0.;
    if (i>515 && i<552) mult[i] = 0.;
    if (i>1208 && i<1232) mult[i] = 0.;
    if (i>1466 && i<1482) mult[i] = 0.;
    if (i>1777 && i<1794) mult[i] = 0.;
    if (i>1888) mult[i] = 0.;
    if (mult[i]==0.) mlt[i]=64.;
    else mlt[i] = 0.;    
  }

  // file for logging flagged spectra
  fitsfile *fptr;
  char fitsnam[100];
  int status=0;
  int rownum = 1;
  time_t rawtime;
  struct tm *info;
  time(&rawtime);
  info = localtime(&rawtime);
  double MJD = (double)(57754.+info->tm_yday+(info->tm_hour+8.)/24.+info->tm_min/(24.*60.)+info->tm_sec/(24.*60.*60.));
  sprintf(fitsnam,"/mnt/nfs/data/%s_%.3lf.fits",fnam,MJD);
  char *ttype[] = {"Spectra","Perc_ts","Perc_samp"};
  char *tform[] = {"2048E", "E", "E"}; 
  char *tunit[] = {"\0", "\0", "\0"};
  char extname[] = "spec_log";
  fits_create_file(&fptr, fitsnam, &status);
  if (status) cerr << "create_file FITS error " << status << endl;
  fits_create_tbl(fptr, BINARY_TBL, 0, 3, ttype, tform, tunit, extname, &status);
  if (status) cerr << "create_tbl FITS error " << status << endl;
  fits_write_key(fptr, TDOUBLE, "MJD", &MJD, "Start MJD", &status);
  float mytsamp = NSAMPS_GULP*1.31072e-4;
  fits_write_key(fptr, TFLOAT, "TSAMP", &mytsamp, "Sample time (s)", &status);

  if (status) cerr << "FITS error " << status << endl;
  else
    cout << "Opened FITS file " << fitsnam << endl;
  fits_close_file(fptr, &status);
  float out_bp[2048], out_pts[1], out_psamp[1];

  // output filterbank file if needed
  if (filty) {

    char filnam[300];
    sprintf(filnam,"/mnt/nfs/data/%s_%.3lf.fil",fnam,MJD);
    if (!(output = fopen(filnam,"wb"))) {
      printf("Couldn't open output file\n");
      return 0;
    }

    send_string("HEADER_START");
    send_string("source_name");
    send_string("TEST");
    send_int("machine_id",1);
    send_int("telescope_id",82);
    send_int("data_type",1); // filterbank data
    send_double("fch1",1530.0); // THIS IS CHANNEL 0 :)
    send_double("foff",-0.1220703125);
    send_int("nchans",2048);
    send_int("nbits",16);
    send_double("tstart",55000.0);
    send_double("tsamp",0.000131072);
    send_int("nifs",1);
    send_string("HEADER_END");
    
  }
  
  multilog(log, LOG_INFO, "main: starting observation\n");

  while (!observation_complete) {

    // open new file if needed.
    if (nints > 4395) {

      rownum = 1;
      nints=0;
      time(&rawtime);
      info = localtime(&rawtime);
      MJD = (double)(57754.+info->tm_yday+(info->tm_hour+8.)/24.+info->tm_min/(24.*60.)+info->tm_sec/(24.*60.*60.));
      sprintf(fitsnam,"/mnt/nfs/data/%s_%.3lf.fits",fnam,MJD);
      char *ttype[] = {"Spectra","Perc_ts","Perc_samp"};
      char *tform[] = {"2048E", "E", "E"}; 
      char *tunit[] = {"\0", "\0", "\0"};
      char extname[] = "spec_log";
      fits_create_file(&fptr, fitsnam, &status);
      if (status) cerr << "create_file FITS error " << status << endl;
      fits_create_tbl(fptr, BINARY_TBL, 0, 3, ttype, tform, tunit, extname, &status);
      if (status) cerr << "create_tbl FITS error " << status << endl;
      fits_write_key(fptr, TDOUBLE, "MJD", &MJD, "Start MJD", &status);
      fits_write_key(fptr, TFLOAT, "TSAMP", &mytsamp, "Sample time (s)", &status);

      if (status) cerr << "FITS error " << status << endl;
      else
	cout << "Opened FITS file " << fitsnam << endl;
      fits_close_file(fptr, &status);

    }

    // read a DADA block

    h_indata = ipcio_open_block_read (hdu_in->data_block, &bytes_read, &block_id);

    // FLAGLEV=0
    if (flaglev==0)
      memcpy(outout_data,h_indata,bytes_read);
    
    
    // deal with zero-ing stuff and setting up bandpasses.
    thrust::fill(h_gulpdata.begin(),h_gulpdata.end(),0);
    clipped = 0;
    clipped_ts = 0;
    for (int i=0;i<NSAMPS_GULP;i++) ts[i] = 0;
    for (int i=0;i<2048;i++) {
      if (started_recording) old_bp[i]=bp[i];
      else old_bp[i] = 0;
      bp[i] = 0;
    }

    // unpack data into h_gulpdata, and find current bp
    for (int k=0;k<NSAMPS_GULP;k++) {
		  
      for (int i=0;i<512;i++) {
	for (int j=0;j<4;j++) {
	  
	  idx = k*4096+i*8+j*2;
	  idxg = k*2048+i*4+j;
	  idxs = i*4+j;
	  tmp=0;
	  tmp |= (unsigned short)(h_indata[idx]) << 8;
	  tmp |= (unsigned short)(h_indata[idx+1]);
	  h_gulpdata[idxg] = (int)tmp;
	  bp[idxs] += (int)tmp;	     	    
	    
	}	
      }
	
    }

    // set up scaling by bandpass
    for (int i=0;i<2048;i++) {
      mmult[i] = mult[i];
      mmlt[i] = mlt[i];
      msc[i] = bp[i]*1.;
      // FLAGLEV=1
      if (old_bp[i]==0) {
	mmult[i] = 0.;
	mmlt[i] = 64.;
      }
      else if (((((float)(bp[i]-old_bp[i]))/((float)(old_bp[i]))>thresh_diff) || (((float)(old_bp[i]-bp[i]))/((float)(old_bp[i]))>thresh_diff)) && flaglev>1) {
	mmult[i] = 0.;
	mmlt[i] = 64.;
      }
    }
    thrust::copy(mmult.begin(),mmult.end(),d_mult.begin());
    thrust::copy(mmlt.begin(),mmlt.end(),d_mlt.begin());
    thrust::copy(msc.begin(),msc.end(),d_sc.begin());
    s_mult = thrust::raw_pointer_cast(&d_mult[0]);
    s_mlt = thrust::raw_pointer_cast(&d_mlt[0]);
    s_sc = thrust::raw_pointer_cast(&d_sc[0]);
    cudaMemcpyToSymbol(s1,&s_mult,sizeof(float *));
    cudaMemcpyToSymbol(s2,&s_sc,sizeof(float *));
    cudaMemcpyToSymbol(s3,&s_mlt,sizeof(float *));
    
    // do bandpass scaling of data
    thrust::copy(h_gulpdata.begin(),h_gulpdata.end(),d_gulpdata.begin());
    thrust::transform(d_gulpdata.begin(),d_gulpdata.end(),d_idx.begin(),d_gulpdata.begin(),da_functor());
    thrust::copy(d_gulpdata.begin(),d_gulpdata.end(),h_gulpdata.begin());

    // copy to out_data, and find ts
    for (int k=0;k<NSAMPS_GULP;k++) {
      for (int i=0;i<2048;i++) {
	idxo = k*2048+i;
	out_data[idxo] = (unsigned short)(h_gulpdata[idxo]);
	ts[k] += h_gulpdata[idxo];
      }
    }

    // FLAGLEV=1
    if (flaglev==1)
      memcpy(outout_data,out_data,bytes_read);

    // do variance flagging
    for (int k=0;k<NSAMPS_GULP;k++) {	 
      for (int i=0;i<2048;i++) {

	tmpval = static_cast<float>((out_data[k*2048+i]-64));
	rmss[i] += tmpval*tmpval;
	    
      }
	  
    }
    for (int i=0;i<2048;i++) {
      rmss[i] = sqrt(rmss[i]/(1.*NSAMPS_GULP));
      if (rmss[i] > 8.6*thresh_rms)  {
	for (int k=0;k<NSAMPS_GULP;k++) 
	  out_data[k*2048+i]=repval;
	rmss[i] = -1.;
      }
    }

    // FLAGLEV=2
    if (flaglev==2)
      memcpy(outout_data,out_data,bytes_read);
    
    // do ts flagging
    for (int k=0;k<NSAMPS_GULP/BT;k++) {

      // time-series flagging
      ts_sum = 0;
      for (int i=k*BT;i<(k+1)*BT;i++)
	ts_sum += ts[i];
      if (ts_sum>thresh_ts) {
	clipped_ts+=BT;
	for (int j=k*BT;j<(k+1)*BT;j++) {
	  for (int i=0;i<2048;i++) 
	    out_data[j*2048+i] = repval;
	}
      }
    }

    // FLAGLEV=3
    if (flaglev==3)
      memcpy(outout_data,out_data,bytes_read);

    // do single-point flagging
    for (int k=0;k<NSAMPS_GULP/BT;k++) {

      for (int i=0;i<2048/BF;i++) {
	ds_sum = 0;
	for (int j=k*BT;j<(k+1)*BT;j++) {
	  for (int l=i*BF;l<(i+1)*BF;l++)
	    ds_sum += out_data[j*2048+l];
	}
	if (ds_sum>clipthresh) {
	  for (int j=k*BT;j<(k+1)*BT;j++) {
	    for (int l=i*BF;l<(i+1)*BF;l++)
	      out_data[j*2048+l] = repval;
	    clipped+=BT*BF;
	  }
	}
      }
      
    }

    // FLAGLEV=4
    if (flaglev==4)
      memcpy(outout_data,out_data,bytes_read);

    // do logging
    fits_open_table(&fptr, fitsnam, READWRITE, &status);
    for (int i=0;i<2048;i++) {
      if (mmult[i]!=0) out_bp[i] = 1.*bp[i];
      else out_bp[i] = -1.*bp[i];
      if (rmss[i]==-1. && out_bp[i]>0.) out_bp[i] = -1.*bp[i];
    }
    out_pts[0] = (float)(100.*clipped_ts/(NSAMPS_GULP));
    out_psamp[0] = (float)(100.*clipped/(NSAMPS_GULP*2048));
    fits_write_col(fptr, TFLOAT, 1, rownum, 1, 2048, out_bp, &status);
    fits_write_col(fptr, TFLOAT, 2, rownum, 1, 1, out_pts, &status);
    fits_write_col(fptr, TFLOAT, 3, rownum, 1, 1, out_psamp, &status);
    if (status) cerr << "FITS error in write " << status << endl;

    rownum += 1;
    fits_update_key(fptr, TINT, "NAXIS2", &rownum, "", &status);
    fits_close_file(fptr, &status);
    nints++;      

    // do the start
    started_recording = 1;
        
    // DO THE WRITING TO BUFFER
    written = ipcio_write (hdu_out->data_block, (char *) outout_data, bytes_to_write);
    
    if (written < bytes_to_write)
      {
	multilog(log, LOG_INFO, "main: failed to write all data to datablock [output]\n");
	dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
	return EXIT_FAILURE;
      }

    // write to filterbank file
    if (filty)
      fwrite(outout_data,sizeof(unsigned short),NSAMPS_GULP*2048,output);

      
  }

  if (filty)
    fclose(output);
  
  dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
  free(out_data);
  free(outout_data);

}

void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out, multilog_t * log)
{

  //dada_cuda_dbunregister (in);
  
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
