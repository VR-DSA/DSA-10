#ifndef __DSAX_DEF_H
#define __DSAX_DEF_H

#include "dada_def.h"

// default port to connect to udpdb command interface
#define DSAX_DEFAULT_UDPDB_PORT   4011
#define DSAX_DEFAULT_PWC_LOGPORT  40123

#define UDP_HEADER   8              // size of header/sequence number
#define UDP_DATA     4096           // obs bytes per packet
#define UDP_PAYLOAD  4104           // header + datasize
#define UDP_IFACE    "10.10.3.1"    // default interface

#endif 

