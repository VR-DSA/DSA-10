/***************************************************************************
 *
 *   Copyright (C) 2012 by Ben Barsdell
 *   Licensed under the Academic Free License version 2.1
 *
 ***************************************************************************/

#include <iostream>
using std::cout;
using std::cerr;
using std::endl;
#include <string>
using std::string;
#include <vector>
using std::vector;
#include <fstream>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <cstdlib> // For atoi
#include <cmath>
#include <algorithm>
#include <inttypes.h>

#include "hd/header.h"
#include "hd/error.h"
#include "hd/default_params.h"
#include "hd/params.h"

#include <dedisp.h>

void usage(char * binary, hd_params params)
{
  cerr << "Usage: " << binary << " [options]" << endl;
  cerr << "  generate the list of DM trials" << endl;
  cerr << "    -f0 f               centre frequency of highest channel [default " << params.f0 << "] MHz" << endl;
  cerr << "    -df f               width of frequecy channel [default " << params.df << "] MHz" << endl;
  cerr << "    -nchan n            number of frequency channels [default " << params.nchans << "]" << endl;
  cerr << "    -dt s               sampling time [default " << params.dt << "] s" << endl;
  cerr << "    -dm min max         DM range [default " << params.dm_min << " " << params.dm_max << "]" << endl;
  cerr << "    -dm_pulse_width w   [default " << params.dm_pulse_width << "] " << endl;
  cerr << "    -dm_tol t           tolerance between DM trials [default " << params.dm_tol << "]" << endl;
}

int main(int argc, char* argv[])
{
  bool verbose = false;
  dedisp_error derror;
  hd_params   params;
  hd_params   default_params;
  hd_set_default_params (&default_params);
  hd_set_default_params (&params);

  size_t i=0;
  while( ++i < (size_t)argc ) 
  {
    if( argv[i] == string("-h") ) {
      usage (argv[0], default_params);
      return -1;
    }

    if (argv[i] == string("-v")) {
      params.verbosity = std::max (params.verbosity, 1);
    }
    else if (argv[i] == string("-f0")) {
      params.f0 = atof(argv[++i]);
    }
    else if (argv[i] == string("-df")) {
      params.df = atof(argv[++i]);
    }
    else if (argv[i] == string("-nchan")) {
      params.nchans = atoi(argv[++i]);
    }
    else if (argv[i] == string("-dt")) {
      params.dt = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm")) {
      params.dm_min = atof(argv[++i]);
      params.dm_max = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm_pulse_width") ) {
      params.dm_pulse_width = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm_tol") ) {
      params.dm_tol = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm_repeater") ) {
     params.dm_repeater_start = atof(argv[++i]);
     params.dm_repeater_stop = atof(argv[++i]);
    }
    else if( argv[i] == string("-n_dms_repeater") ) {
      params.n_dms_repeater = atoi(argv[++i]);
    }
    else {
      cerr << "WARNING: Unknown parameter '" << argv[i] << "'" << endl;
    }
  }

  dedisp_size dm_count;
  const float * dm_list = dedisp_generate_dm_list_guru (params.dm_min,
                                    params.dm_max,
                                    params.dt,
                                    params.dm_pulse_width,
                                    params.f0,
                                    params.df,
                                    params.nchans,
                                    params.dm_tol,
                                    &dm_count);
  if( derror != DEDISP_NO_ERROR ) {
    throw_dedisp_error(derror);
    return 1;
  }

  /*for( hd_size i=0; i<dm_count; ++i ) 
  {
    cout << dm_list[i] << endl;
  }*/

  //if we are searching for a repeater, insert finely spaced dms into the dm list

dedisp_size dm_count2;
  dedisp_plan dedispersion_plan;
  derror = dedisp_create_plan(&dedispersion_plan,params.nchans,params.dt,params.f0,params.df);
  derror = dedisp_generate_dm_list(dedispersion_plan,params.dm_min,params.dm_max,params.dm_pulse_width,params.dm_tol);
  if( derror != DEDISP_NO_ERROR ) {
    throw_dedisp_error(derror);
    return 1;
  }

    const float* dm_list_init  = dedisp_get_dm_list(dedispersion_plan);  //get the old dm list
    hd_size      dm_count_init = dedisp_get_dm_count(dedispersion_plan);
    for (hd_size i = 0; i<dm_count_init; i++) {
      cout << dm_list_init[i] << endl;
    }
    cout << "done with dm_list_init " << endl;
    hd_size      dm_count_repeaters = dm_count_init + params.n_dms_repeater;
    float dm_list_repeaters[dm_count_repeaters];  //initialize the new dm list
    //const float dm_list_repeaters_const[dm_count_repeaters];
    float dm = 0;
    hd_size new_dms = 0; //to keep track of indices
    hd_size repeater_dm_status = 0;
    //loop over the old dm list
    for (hd_size i=0;i<dm_count_init;i++)  {
      dm = dm_list_init[i];
      //look for the dm where you want to start looking for repeater dms
      if (dm >= params.dm_repeater_start && params.dm_repeater_start > dm_list_init[i-1] )  {
        repeater_dm_status = 1; //starting to insert finely spaced dms
        double repeater_dm_spacing = ((double)(params.dm_repeater_stop-params.dm_repeater_start)/(double)(params.n_dms_repeater-1));
        // insert the finely spaced dms
        for (hd_size j=0; j< params.n_dms_repeater;j++)  {
          float dm_repeater = params.dm_repeater_start + repeater_dm_spacing*j;
          dm_list_repeaters[i+j] = dm_repeater;
        }
      }
      if (repeater_dm_status == 0) {
        dm_list_repeaters[i] = dm; //if we haven't inserted dms yet, dm list is the same
        new_dms += 1;
     }
     //if there are dms in the old dm list that are between dm_repeater_start and dm_repeater_stop, skip them
      else if (repeater_dm_status == 1) {
        if (dm > params.dm_repeater_stop) repeater_dm_status = 2;
      }
      //once the repeater dms have been inserted, the indexing needs to change to continue the dm list
      else if (repeater_dm_status == 2) {
        dm_list_repeaters[new_dms+params.n_dms_repeater] = dm;
        new_dms += 1;
      }
    }
  hd_size new_dm_count_repeaters = new_dms+params.n_dms_repeater; //final number of dms
  //for (hd_size i = 0; i<new_dm_count_repeaters; i++) {
  //  dm_list_repeaters_const[i] = (const float)(dm_list_repeaters[i]);
  //}
  const float * dm_list_repeaters_const = dm_list_repeaters;
  derror = dedisp_set_dm_list(dedispersion_plan,
                              dm_list_repeaters_const,
                              new_dm_count_repeaters);
  if( derror != DEDISP_NO_ERROR ) {
    return throw_dedisp_error(derror);
  }
  const float * final_dm_list = dedisp_get_dm_list(dedispersion_plan);
  for (hd_size i = 0; i<new_dm_count_repeaters; i++) {
    cout << final_dm_list[i] << endl;
  }

/*  dedisp_size dm_count2;
  dedisp_plan dedispersion_plan;
  derror = dedisp_create_plan(&dedispersion_plan,params.nchans,params.dt,params.f0,params.df);
  if( derror != DEDISP_NO_ERROR ) {
    throw_dedisp_error(derror);
    return 1;
  }

  derror = dedisp_generate_dm_list_repeaters (dedispersion_plan,
				    params.dm_min,
                                    params.dm_max,
                                    params.dm_pulse_width,
                                    params.dm_tol,
				    params.dm_repeater_start,
				    params.dm_repeater_stop,
                                    params.n_dms_repeater);

dm_count2 = dedisp_get_dm_count(dedispersion_plan);
const float* dm_list2  = dedisp_get_dm_list(dedispersion_plan);

  if( derror != DEDISP_NO_ERROR ) {
    throw_dedisp_error(derror);
    return 1;
  }
  

  for( hd_size i=0; i<dm_count2; ++i )
  {
    cout << dm_list2[i] << endl;
  }*/
}
