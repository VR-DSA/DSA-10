import sys, subprocess, os
from time import sleep

dada_nbytes = '960000'
eada_nbytes = '7680000'

machine='dsa5'

dbuffers=0
cbuffers=0
start=0
stop=0
if (sys.argv[1]=='destroy'):
    dbuffers=1
if (sys.argv[1]=='create'):
    cbuffers=1
if (sys.argv[1]=='start'):
    start=1
if (sys.argv[1]=='stop'):
    stop=1
    
# dada buffers

if dbuffers:
    
    print 'destroying dada buffers on ',machine
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbd -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adcd -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k addd -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k aaaa -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k aaba -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k eada -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k fada -d"')
    

if cbuffers:
    print 'creating dada buffers on ',machine
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbd -b 192000 -l -p -c 0 -n 10"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adcd -b 192000 -l -p -c 0 -n 10"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k addd -b 192000 -l -p -c 0 -n 10"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k aaaa -b 192000 -l -p -c 0 -n 10"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k aaba -b 192000 -l -p -c 0 -n 10"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k eada -b 7680000 -l -p -c 0 -n 2"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k fada -b 7680000 -l -p -c 0 -n 2"')
   


# processes


ndb1 = '/mnt/nfs/code/dsaX/cohsrc/dsaX_nicdb_tcp -c 1 -b 192000 -i 10.10.4.6 -p 4011 -k adbd'
ndb2 = '/mnt/nfs/code/dsaX/cohsrc/dsaX_nicdb_tcp -c 2 -b 192000 -i 10.10.4.6 -p 4012 -k adcd'
ndb3 = '/mnt/nfs/code/dsaX/cohsrc/dsaX_nicdb_tcp -c 4 -b 192000 -i 10.10.4.6 -p 4013 -k addd'
ndb4 = '/mnt/nfs/code/dsaX/cohsrc/dsaX_nicdb_tcp -c 6 -b 192000 -i 10.10.4.6 -p 4014 -k aaaa'
ndb5 = '/mnt/nfs/code/dsaX/cohsrc/dsaX_nicdb_tcp -c 7 -b 192000 -i 10.10.4.6 -p 4015 -k aaba'
ftus = '/mnt/nfs/code/dsaX/cohsrc/dsaX_ftus -c 3 -f /mnt/nfs/code/dsaX/cohsrc/spectrometer_header.txt'
flagg = '/mnt/nfs/code/dsaX/cohsrc/dsaX_flag -c 20'
heimdall = '/usr/local/heimdallr/bin/heimdall -k fada -v -nsamps_gulp 6144 -output_dir /mnt/nfs/data/heimdall -dm 0 3000'
cpheim = '/mnt/nfs/code/dsaX/cohsrc/cpheimdall.bash'

# start things up

if start:

    print 'Starting cpheim'
    cpheim_log = open('/mnt/nfs/runtime/tmplog/cpheim_log_'+machine+'.log','w')
    cpheim_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+cpheim+'"', shell = True, stdout=cpheim_log, stderr=cpheim_log)
    sleep(0.1)
    
    print 'Starting heimdall'
    heimdall_log = open('/mnt/nfs/runtime/tmplog/heimdall_log_'+machine+'.log','w')
    heimdall_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+heimdall+'"', shell = True, stdout=heimdall_log, stderr=heimdall_log)
    sleep(0.1)

    print 'Starting flag'
    flag_log = open('/mnt/nfs/runtime/tmplog/flag_log_'+machine+'.log','w')
    flag_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+flagg+'"', shell = True, stdout=flag_log, stderr=flag_log)
    sleep(0.1)
    
    print 'Starting ftus'
    ftus_log = open('/mnt/nfs/runtime/tmplog/ftus_log_'+machine+'.log','w')
    ftus_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+ftus+'"', shell = True, stdout=ftus_log, stderr=ftus_log)
    sleep(0.1)

    print 'Starting captures'
    ndb1_log = open('/mnt/nfs/runtime/tmplog/ndb1_log_'+machine+'.log','w')
    ndb1_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+ndb1+'"', shell = True, stdout=ndb1_log, stderr=ndb1_log)
    ndb2_log = open('/mnt/nfs/runtime/tmplog/ndb2_log_'+machine+'.log','w')
    ndb2_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+ndb2+'"', shell = True, stdout=ndb2_log, stderr=ndb2_log)
    ndb3_log = open('/mnt/nfs/runtime/tmplog/ndb3_log_'+machine+'.log','w')
    ndb3_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+ndb3+'"', shell = True, stdout=ndb3_log, stderr=ndb3_log)
    ndb4_log = open('/mnt/nfs/runtime/tmplog/ndb4_log_'+machine+'.log','w')
    ndb4_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+ndb4+'"', shell = True, stdout=ndb4_log, stderr=ndb4_log)
    ndb5_log = open('/mnt/nfs/runtime/tmplog/ndb5_log_'+machine+'.log','w')
    ndb5_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+ndb5+'"', shell = True, stdout=ndb5_log, stderr=ndb5_log)
    
    
if stop:

    print 'Killing everything'

    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q cpheimdall.bash"',shell=True)
    subprocess.Popen.wait(output0)
    
    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q dsaX_nicdb_tcp"',shell=True)
    subprocess.Popen.wait(output0)

    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q dsaX_ftus"',shell=True)
    subprocess.Popen.wait(output0)

    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q dsaX_flag"',shell=True)
    subprocess.Popen.wait(output0)

    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q heimdall"',shell=True)
    subprocess.Popen.wait(output0)
