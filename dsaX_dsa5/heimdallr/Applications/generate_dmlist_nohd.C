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

//#include "hd/header.h"
//#include "hd/error.h"
//#include "hd/default_params.h"
//#include "hd/params.h"

#include <dedisp.h>

/*void usage(char * binary, hd_params params)
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
}*/

int main(int argc, char* argv[])
{
  bool verbose = false;
  dedisp_error derror;
  //hd_params   params;
  //hd_params   default_params;
  //hd_set_default_params (&default_params);
  //hd_set_default_params (&params);

  int verbosity = 0;
  float f0 = 1530.;
  float df = 250./2048;
  int nchan = 2048;
  float dt = 131.072e-6;
  float dm_min = 70;
  float dm_max = 2000;
  float dm_pulse_width = 40e-6;
  float dm_tol = 1.25;
  float dm_repeater_start = 170;
  float dm_repeater_stop = 200;
  unsigned long n_dms_repeater = 50;
  unsigned long nchans=2048;

  size_t i=0;
  while( ++i < (size_t)argc ) 
  {
    //if( argv[i] == string("-h") ) {
    //  usage (argv[0], default_params);
    //  return -1;
    //}

    if (argv[i] == string("-v")) {
      verbosity = std::max (verbosity, 1);
    }
    else if (argv[i] == string("-f0")) {
      f0 = atof(argv[++i]);
    }
    else if (argv[i] == string("-df")) {
      df = atof(argv[++i]);
    }
    else if (argv[i] == string("-nchan")) {
      nchans = atoi(argv[++i]);
    }
    else if (argv[i] == string("-dt")) {
      dt = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm")) {
      dm_min = atof(argv[++i]);
      dm_max = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm_pulse_width") ) {
      dm_pulse_width = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm_tol") ) {
      dm_tol = atof(argv[++i]);
    }
    else if( argv[i] == string("-dm_repeater") ) {
     dm_repeater_start = atof(argv[++i]);
     dm_repeater_stop = atof(argv[++i]);
    }
    else if( argv[i] == string("-n_dms_repeater") ) {
      n_dms_repeater = atoi(argv[++i]);
    }
    else {
      cerr << "WARNING: Unknown parameter '" << argv[i] << "'" << endl;
    }
  }

  dedisp_size dm_count;
  const float * dm_list = dedisp_generate_dm_list_guru ( dm_min,
                                     dm_max,
                                     dt,
                                     dm_pulse_width,
                                     f0,
                                     df,
                                     nchans,
                                     dm_tol,
                                    &dm_count);

  for( int i=0; i<dm_count; ++i ) 
  {
    cout << dm_list[i] << endl;
  }

  dedisp_size dm_count2;
  dedisp_plan dedispersion_plan;
  derror = dedisp_create_plan(&dedispersion_plan, nchans, dt, f0, df);

  derror = dedisp_generate_dm_list_repeaters (dedispersion_plan,
				     dm_min,
                                     dm_max,
                                     dm_pulse_width,
                                     dm_tol,
				     dm_repeater_start,
				     dm_repeater_stop,
                                     n_dms_repeater);

dm_count2 = dedisp_get_dm_count(dedispersion_plan);
const float* dm_list2  = dedisp_get_dm_list(dedispersion_plan);

  

  for( int i=0; i<dm_count2; ++i )
  {
    cout << dm_list2[i] << endl;
  }
}
