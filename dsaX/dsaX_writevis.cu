// -*- c++ -*-
#include <iostream>
#include <algorithm>
using std::cout;
using std::cerr;
using std::endl;
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

#include "dada_client.h"
#include "dada_def.h"
#include "dada_hdu.h"
#include "multilog.h"
#include "ipcio.h"
#include "ipcbuf.h"
#include "dada_affinity.h"
#include "ascii_header.h"

#include "fitsio.h"

#define NINTS 100
#define NCALCS 100
const double diffobs = 3000.0;

float summed_vis[27500];

void dsaX_dbgpu_cleanup (dada_hdu_t * in, dada_hdu_t * out, multilog_t * log);

int docrab(double *crabs, double samp);
int docal(double *cals, double samp);

int docrab(double *crabs, double samp) {

  for (int i=0;i<NCALCS;i++)
    if ((crabs[i]-samp<diffobs) && (crabs[i]-samp>-diffobs)) return 1;

  return 0;

}

int docal(double *cals, double samp) {

  for (int i=0;i<NCALCS;i++)
    if ((cals[i]-samp<diffobs) && (cals[i]-samp>-diffobs)) return 1;

  return 0;

}

void usage()
{
  fprintf (stdout,
	   "dsaX_image [options]\n"
	   " -c core   bind process to CPU core\n"
	   " -f filename [default test.fits]\n"
	   " -o freq of chan 1 [default 1530.0]\n"
	   " -h        print usage\n");
}

void dsaX_dbgpu_cleanup (dada_hdu_t * in, multilog_t * log) {

  if (dada_hdu_unlock_read (in) < 0)
    {
      multilog(log, LOG_ERR, "could not unlock read on hdu_in\n");
    }
  dada_hdu_destroy (in);

}

int main (int argc, char *argv[]) {

  /* DADA defs */
  dada_hdu_t* hdu_in = 0;
  multilog_t* log = 0;
  key_t in_key = 0x0000eada;

  // load in mjds
  FILE *fin;
  double *cals;
  crabs = (double *)malloc(sizeof(double)*NCALCS);
  cals = (double *)malloc(sizeof(double)*NCALCS);
  fin=fopen("/mnt/nfs/runtime/crab_mjds.dat","r");
  for (int i=0;i<NCALCS;i++) fscanf(fin,"%lf\n",&crabs[i]);
  fclose(fin);
  fin=fopen("/mnt/nfs/runtime/cal_mjds.dat","r");
  for (int i=0;i<NCALCS;i++) fscanf(fin,"%lf\n",&cals[i]);
  fclose(fin);
  cout << "Read Crab and cal MJDs" << endl;
  
  // command line
  int arg = 0;
  int core = -1;
  int nsamps = 384;
  float fch1 = 1530.0;
  int npts = 55*125*2*2;
  int nsamps_gulp = 384;
  char fnam[300], foutnam[400];
  sprintf(fnam,"/mnt/nfs/data/alltest");
  
  while ((arg=getopt(argc,argv,"c:f:o:h")) != -1)
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
	case 'h':
	  usage();
	  return EXIT_SUCCESS;
	}
    }

  // DADA stuff
  
  log = multilog_open ("dsaX_image", 0);
  
  multilog_add (log, stderr);

  multilog (log, LOG_INFO, "dsaX_image: creating hdu\n");

  hdu_in  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_in, in_key);
  if (dada_hdu_connect (hdu_in) < 0) {
    printf ("dsaX_image: could not connect to dada buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_read (hdu_in) < 0) {
    printf ("dsaX_image: could not lock to dada buffer\n");
    return EXIT_FAILURE;
  }

  hdu_out  = dada_hdu_create (log);
  dada_hdu_set_key (hdu_out, out_key);
  if (dada_hdu_connect (hdu_out) < 0) {
    printf ("dsaX_ftus: could not connect to output  buffer\n");
    return EXIT_FAILURE;
  }
  if (dada_hdu_lock_write(hdu_out) < 0) {
    fprintf (stderr, "dsaX_ftus: could not lock to output buffer\n");
    return EXIT_FAILURE;
  }

  // Bind to cpu core
  if (core >= 0)
    {
      printf("binding to core %d\n", core);
      if (dada_bind_thread_to_core(core) < 0)
	printf("dsaX_image: failed to bind to core %d\n", core);
    }

  int observation_complete=0;

  // more DADA stuff - deal with headers
  
  uint64_t header_size = 0;

  // read the headers from the input HDUs and mark as cleared
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

  // set up
  FILE *ftime;
  int fctr = 0, integration = 0;
  char tstamp[100];
  double mjd, samp = 0.0;
  fitsfile *fptr;
  int rownum = 1;
  int fwrite = 0;
  int status=0;
  int crabtime = 0, caltime = 0;
  
  // data stuff
  uint64_t block_size = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_in->data_block);
  uint64_t out_size = ipcbuf_get_bufsz ((ipcbuf_t *) hdu_out->data_block);
  uint64_t bytes_read = 0, block_id, written;
  char *block;
  float  *out_block;
  out_block = (float *)malloc(sizeof(float)*out_size/4);
  float *data, m1r, m1i, m2r, m2i;

  // mean vis, and vis def
  float ****mvis, *****vis;
  mvis = (float ****)malloc(sizeof(float ***)*45);
  for (int i=0;i<45;i++) {
    mvis[i] = (float ***)malloc(sizeof(float **)*125);
    for (int j=0;j<125;j++) {
      mvis[i][j] = (float **)malloc(sizeof(float *)*2);
      for (int k=0;k<2;k++) {
	mvis[i][j][k] = (float *)malloc(sizeof(float)*2);
	mvis[i][j][k][0] = 0.;
	mvis[i][j][k][1] = 0.;
      }
    }
  }
  vis = (float *****)malloc(sizeof(float ****)*nsamps);
  for (int ti=0;ti<nsamps;ti++) {
    vis[ti] = (float ****)malloc(sizeof(float ***)*45);
    for (int i=0;i<45;i++) {
      vis[ti][i] = (float ***)malloc(sizeof(float **)*125);
      for (int j=0;j<125;j++) {
	vis[ti][i][j] = (float **)malloc(sizeof(float *)*2);
	for (int k=0;k<2;k++) {
	  vis[ti][i][j][k] = (float *)malloc(sizeof(float)*2);
	}
      }
    }
  }
  int mblock=10, bi;


  // set up lookup table for triplets

  // find indices of actual baselines
  int bases[45], a1[45], a2[45];
  int bct = 0, allct = 0;
  for (int i=0;i<10;i++) {
    for (int j=0;j<=i;j++) {
      if (i!=j) {
	bases[bct] = allct;
	a1[bct] = j;
	a2[bct] = i;
	bct++;
      }
      allct++;
    }
  }
  cout << "printing all baseline numbers ";
  for (int i=0;i<45;i++) cout << bases[i] << " " << a1[i] << " " << a2[i] << endl;
  cout << endl;
  bct = 0;

  // brute force all triplets
  int trips[120][3], tct = 0;
  for (int i=0;i<10;i++) {
    for (int j=0;j<i;j++) {
      for (int k=0;k<j;k++) {

	//triplet is from antennas i,j,k
	
	// loop over bases three times to find correct baselines

	for (int l=0;l<45;l++) {
	  if ((a1[l]==i && a2[l]==j) || (a1[l]==j && a2[l]==i))
	    trips[tct][0] = l;
	}

	for (int l=0;l<45;l++) {
	  if ((a1[l]==i && a2[l]==k) || (a1[l]==k && a2[l]==i))
	    trips[tct][1] = l;
	}

	for (int l=0;l<45;l++) {
	  if ((a1[l]==k && a2[l]==j) || (a1[l]==j && a2[l]==k))
	    trips[tct][2] = l;
	}

	tct++;
	
      }
    }
  }

  cout << "printing all triplet numbers ";
  for (int i=0;i<120;i++) cout << trips[i][0] << " " << trips[i][1] << " " << trips[i][2] << endl;
  cout << endl;
 
  
  // start things

  multilog(log, LOG_INFO, "dsaX_image: starting observation\n");

  while (!observation_complete) {

    block = ipcio_open_block_read (hdu_in->data_block, &bytes_read, &block_id);
    data = (float *)block; // order is [384 time, 55 baseline, 125 freq, 2 pol, 2 ri]

    // sum input visibilities
    for (int i=0;i<npts;i++) summed_vis[i] = 0.;
    for (int i=0;i<nsamps_gulp;i++) {
      for (int j=0;j<npts;j++)
	summed_vis[j] += data[i*npts+j];
    }

    // three file writing cases: initial (fctr=0), crab and cal (fctr > 0)

    // CASE FOR CRAB OR CAL FILE

    if (fctr > 0) {

      // check for source
      crabtime = docrab(crabs,samp);
      caltime = docal(cals,samp);
      if (crabtime || caltime) fwrite = 1;
      else {
	if (fwrite == 1) {
	  integration = 0;
	  cout << "Completed file " << fctr << endl;
	  fctr++;
	}
	fwrite = 0;
      }
      

      // do file writing
      if (fwrite) {

	if (crabtime) multilog(log, LOG_INFO, "dsaX_final: crabtime\n");
	if (caltime) multilog(log, LOG_INFO, "dsaX_final: caltime\n");

	// create file
	if (integration==0) {
	
	  if (crabtime) {
	    sprintf(foutnam,"%s_%s_crab_%d.fits",fnam,tstamp,fctr);
	  }
	  if (caltime) {
	    sprintf(foutnam,"%s_%s_cal_%d.fits",fnam,tstamp,fctr);
	  }
	  cout << "main: opening new file " << foutnam << endl;
	  rownum=1;
	
	  char *ttype[] = {"VIS"};
	  char *tform[] = {"27500E"}; // assumes classic npts
	  char *tunit[] = {"\0"};
	  char *antennas = "3-7-2-10-1-4-5-8-6-9";
	
	  
	  char extname[] = "DATA";
	  fits_create_file(&fptr, foutnam, &status);
	  if (status) cerr << "create_file FITS error " << status << endl;
	  fits_create_tbl(fptr, BINARY_TBL, 0, 1, ttype, tform, tunit, extname, &status);
	  if (status) cerr << "create_tbl FITS error " << status << endl;
	  fits_write_key(fptr, TDOUBLE, "MJD", &mjd, "Start MJD", &status);
	  float mytsamp = nsamps_gulp*8.192e-6*128.;
	  fits_write_key(fptr, TFLOAT, "TSAMP", &mytsamp, "Sample time (s)", &status);
	  fits_write_key(fptr, TFLOAT, "FCH1", &fch1, "Frequency (MHz)", &status);
	  fits_write_key(fptr, TSTRING, "Antennas", &antennas[0], "Antennas", &status);
	  
	  if (status) cerr << "FITS error " << status << endl;
      
	  fits_close_file(fptr, &status);

	}
      
	// write to file
   
	fits_open_table(&fptr, foutnam, READWRITE, &status);
	fits_write_col(fptr, TFLOAT, 1, rownum, 1, 66000, summed_vis, &status);
	if (status) cerr << "FITS error in write " << status << endl;
	rownum += 1;
	fits_update_key(fptr, TINT, "NAXIS2", &rownum, "", &status);
	fits_close_file(fptr, &status);
	integration++;

      }
      
    }

    // CASE FOR FIRST FILE
    else if (fctr==0) {

      // if first integration of first file
      if (samp == 0) {
	
	// get start time, and convert others to samples
	ftime=fopen("/mnt/nfs/runtime/UTC_START.txt","r");
	fscanf(ftime,"%lf\n",&mjd);
	fscanf(ftime,"%[^\n]",&tstamp[0]);
	fclose(ftime);

	for (int i=0;i<NCALCS;i++) {
	  crabs[i] = (crabs[i]-mjd)*86400./(nsamps_gulp*8.192e-6*128.);
	  cals[i] = (cals[i]-mjd)*86400./(nsamps_gulp*8.192e-6*128.);
	}

	// open file
	
	sprintf(foutnam,"%s_%s_%d.fits",fnam,tstamp,fctr);
	cout << "main: opening new file " << foutnam << endl;
	rownum=1;
	
	char *ttype[] = {"VIS"};
	char *tform[] = {"27500E"}; // assumes classic npts
	char *tunit[] = {"\0"};
	char *antennas = "3-7-2-10-1-4-5-8-6-9";
	  
	char extname[] = "DATA";
	fits_create_file(&fptr, foutnam, &status);
	if (status) cerr << "create_file FITS error " << status << endl;
	fits_create_tbl(fptr, BINARY_TBL, 0, 1, ttype, tform, tunit, extname, &status);
	if (status) cerr << "create_tbl FITS error " << status << endl;
	fits_write_key(fptr, TDOUBLE, "MJD", &mjd, "Start MJD", &status);
	float mytsamp = nsamps_gulp*8.192e-6*128.;
	fits_write_key(fptr, TFLOAT, "TSAMP", &mytsamp, "Sample time (s)", &status);
	fits_write_key(fptr, TFLOAT, "FCH1", &fch1, "Frequency (MHz)", &status);
	fits_write_key(fptr, TSTRING, "Antennas", &antennas[0], "Antennas", &status);

	if (status) cerr << "FITS error " << status << endl;
      
	fits_close_file(fptr, &status);

      }
      
      // write to file
   
      fits_open_table(&fptr, foutnam, READWRITE, &status);
      fits_write_col(fptr, TFLOAT, 1, rownum, 1, 66000, summed_vis, &status);
      if (status) cerr << "FITS error in write " << status << endl;
      rownum += 1;
      fits_update_key(fptr, TINT, "NAXIS2", &rownum, "", &status);
      fits_close_file(fptr, &status);
      integration++;

      if (integration==NINTS) {
	integration=0;
	cout << "Completed file " << fctr << endl;
	fctr++;
      }
      
    }

    // update mjd and samp
    mjd += nsamps_gulp*128.*8.192e-6/86400.;
    samp += 1.0;
    
    // find mean vis
    if (bct<mblock) {

      multilog(log, LOG_INFO, "dsaX_image: finding mean vis %d of %d\n",bct+1,mblock);
      
      for (int t_idx=0;t_idx<nsamps;t_idx++) {
	for (int b_idx=0;b_idx<45;b_idx++) {
	  bi = bases[b_idx];
	  for (int f_idx=0;f_idx<125;f_idx++) {
	    for (int p_idx=0;p_idx<2;p_idx++) {
	      for (int r_idx=0;r_idx<2;r_idx++) {

		mvis[b_idx][f_idx][p_idx][r_idx] += data[t_idx*27500+bi*500+f_idx*4+p_idx*2+r_idx];
		
	      }
	    }
	  }
	}
      }
      for (int b_idx=0;b_idx<45;b_idx++) {
	bi = bases[b_idx];
	for (int f_idx=0;f_idx<125;f_idx++) {
	  for (int p_idx=0;p_idx<2;p_idx++) {
	    for (int r_idx=0;r_idx<2;r_idx++) {
	      
	      mvis[b_idx][f_idx][p_idx][r_idx] /= 1.*nsamps*mblock;
	      
	    }
	  }
	}
      }

      bct++;
      
    }
    // do bispectrum
    else {
    
      // subtract mean vis
      for (int t_idx=0;t_idx<nsamps;t_idx++) {
	for (int b_idx=0;b_idx<45;b_idx++) {
	  bi = bases[b_idx];
	  for (int f_idx=0;f_idx<125;f_idx++) {
	    for (int p_idx=0;p_idx<2;p_idx++) {
	      for (int r_idx=0;r_idx<2;r_idx++) {

		vis[t_idx][b_idx][f_idx][p_idx][r_idx] = data[t_idx*27500+bi*500+f_idx*4+p_idx*2+r_idx]-mvis[b_idx][f_idx][p_idx][r_idx];
		
	      }
	    }
	  }
	}
      }
    
      // for each time bin, sum over triple product of each baseline triplet. 
      // indices for baselines to use are in trips

      // form bispectrum as two complex multiply steps.
      // m1_r = ar*br - ai*bi, m1_i = ar*bi + ai*br
      // m2_r = m1_r*cr + m1_i*ci, m2_i = m1_i*cr - m1_r*ci

      // main loop
      for (int t_idx=0;t_idx<nsamps;t_idx++) {
	
	for (int f_idx=0;f_idx<125;f_idx++) {

	  out_block[t_idx*125+f_idx] = 0.;

	  for (int trip_idx=0;trip_idx<120;trip_idx++) {
	    for (int p_idx=0;p_idx<2;p_idx++) {

	      m1r = vis[t_idx][trips[trip_idx][0]][f_idx][p_idx][0] * vis[t_idx][trips[trip_idx][1]][f_idx][p_idx][0] - vis[t_idx][trips[trip_idx][0]][f_idx][p_idx][1] * vis[t_idx][trips[trip_idx][1]][f_idx][p_idx][1];
	      m1i = vis[t_idx][trips[trip_idx][0]][f_idx][p_idx][0] * vis[t_idx][trips[trip_idx][1]][f_idx][p_idx][1] + vis[t_idx][trips[trip_idx][0]][f_idx][p_idx][1] * vis[t_idx][trips[trip_idx][1]][f_idx][p_idx][0];
	      m2r = m1r * vis[t_idx][trips[trip_idx][2]][f_idx][p_idx][0] + m1i * vis[t_idx][trips[trip_idx][2]][f_idx][p_idx][1];
	      m2i = -m1r * vis[t_idx][trips[trip_idx][2]][f_idx][p_idx][1] + m1i * vis[t_idx][trips[trip_idx][2]][f_idx][p_idx][0];
	      
	      out_block[t_idx*125+f_idx] += sqrt(m2r*m2r+m2i*m2i);

	    }
	  }
	}
	
      }

      // write out_block
      written = ipcio_write (hdu_out->data_block, (char *)(out_block), out_size);
      if (written < out_size)
	{
	  multilog(log, LOG_INFO, "main: failed to write all data to datablock [output]\n");
	  dsaX_dbgpu_cleanup (hdu_in, hdu_out, log);
	  return EXIT_FAILURE;
	}
      
      }
        
    // close off loop
    if (bytes_read < block_size)
      observation_complete = 1;

    ipcio_close_block_read (hdu_in->data_block, bytes_read);
    
  }

  free(mvis);
  free(vis);
  free(crabs);
  free(cals);
  dsaX_dbgpu_cleanup(hdu_in, hdu_out, log);
 
}
