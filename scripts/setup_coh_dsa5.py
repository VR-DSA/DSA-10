import sys, subprocess, os
from time import sleep


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
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k abda -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k bbda -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k cbda -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k dbda -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k ebda -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adba -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbb -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbc -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbd -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbe -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k dada -d"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k eada -d"')
    

if cbuffers:
    print 'creating dada buffers on ',machine
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k abda -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k bbda -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k cbda -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k dbda -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k ebda -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adba -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbb -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbc -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbd -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k adbe -b 256000000 -l -p -c 0 -n 4"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k dada -b 256000000 -l -p -c 0 -n 32"')
    os.system('ssh user@'+machine+' "source ~/.bashrc; dada_db -k eada -b 256000000 -l -p -c 0 -n 4"')
    
   

 
# processes

capture1 = '/usr/local/dsaX/bin/dsaX_spectrometer_udpdb_thread -b 16 -f /mnt/nfs/code/dsaX_future/DSA-10/dsaX_dsa5/accum/spectrometer_header.txt -k abda -i 10.10.3.1 -p 4011'
capture2 = '/usr/local/dsaX/bin/dsaX_spectrometer_udpdb_thread -b 17 -f /mnt/nfs/code/dsaX_future/DSA-10/dsaX_dsa5/accum/spectrometer_header.txt -k bbda -i 10.10.3.1 -p 4012'
capture3 = '/usr/local/dsaX/bin/dsaX_spectrometer_udpdb_thread -b 18 -f /mnt/nfs/code/dsaX_future/DSA-10/dsaX_dsa5/accum/spectrometer_header.txt -k cbda -i 10.10.3.1 -p 4013'
capture4 = '/usr/local/dsaX/bin/dsaX_spectrometer_udpdb_thread -b 19 -f /mnt/nfs/code/dsaX_future/DSA-10/dsaX_dsa5/accum/spectrometer_header.txt -k dbda -i 10.10.3.1 -p 4014'
capture5 = '/usr/local/dsaX/bin/dsaX_spectrometer_udpdb_thread -b 20 -f /mnt/nfs/code/dsaX_future/DSA-10/dsaX_dsa5/accum/spectrometer_header.txt -k ebda -i 10.10.3.1 -p 4015'
filflag1 = '/usr/local/dsaX/bin/dsaX_filflag -c 2 -f 1 -n slog1 -k abda -l adba'
filflag2 = '/usr/local/dsaX/bin/dsaX_filflag -c 3 -f 1 -n slog2 -k bbda -l adbb'
filflag3 = '/usr/local/dsaX/bin/dsaX_filflag -c 4 -f 1 -n slog3 -k cbda -l adbc'
filflag4 = '/usr/local/dsaX/bin/dsaX_filflag -c 5 -f 1 -n slog4 -k dbda -l adbd'
filflag5 = '/usr/local/dsaX/bin/dsaX_filflag -c 6 -f 1 -n slog5 -k ebda -l adbe'
dbmerge = '/usr/local/dsaX/bin/dsaX/dbmerge -c 7 -db0 adba -db1 adbb -db2 adbc -db3 adbd -db4 adbe -o dada'
final = '/usr/local/dsaX/bin/dsaX_filflag -c 1 -f 0 -n blog -k dada -l eada'
dbnull = 'dada_dbnull -k eada'


# start things up

if start:

    print 'Starting dbnull'
    cpheim_log = open('/mnt/nfs/runtime/tmplog/dbnull_log_'+machine+'.log','w')
    cpheim_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+dbnull+'"', shell = True, stdout=dbnull_log, stderr=dbnull_log)
    sleep(0.1)
    
    print 'Starting final'
    final_log = open('/mnt/nfs/runtime/tmplog/final_log_'+machine+'.log','w')
    final_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+final+'"', shell = True, stdout=final_log, stderr=final_log)
    sleep(0.1)

    print 'Starting dbmerge'
    dbmerge_log = open('/mnt/nfs/runtime/tmplog/dbmerge_log_'+machine+'.log','w')
    dbmerge_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+dbmerge+'"', shell = True, stdout=dbmerge_log, stderr=dbmerge_log)
    sleep(0.1)

    print 'Starting 5 filflags'
    filflag1_log = open('/mnt/nfs/runtime/tmplog/filflag1_log_'+machine+'.log','w')
    filflag1_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+filflag1+'"', shell = True, stdout=filflag1_log, stderr=filflag1_log)
    sleep(0.1)
    filflag2_log = open('/mnt/nfs/runtime/tmplog/filflag2_log_'+machine+'.log','w')
    filflag2_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+filflag2+'"', shell = True, stdout=filflag2_log, stderr=filflag2_log)
    sleep(0.1)
    filflag3_log = open('/mnt/nfs/runtime/tmplog/filflag3_log_'+machine+'.log','w')
    filflag3_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+filflag3+'"', shell = True, stdout=filflag3_log, stderr=filflag3_log)
    sleep(0.1)
    filflag4_log = open('/mnt/nfs/runtime/tmplog/filflag4_log_'+machine+'.log','w')
    filflag4_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+filflag4+'"', shell = True, stdout=filflag4_log, stderr=filflag4_log)
    sleep(0.1)
    filflag5_log = open('/mnt/nfs/runtime/tmplog/filflag5_log_'+machine+'.log','w')
    filflag5_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+filflag5+'"', shell = True, stdout=filflag5_log, stderr=filflag5_log)
    sleep(0.1)

    print 'Starting 5 captures'
    capture1_log = open('/mnt/nfs/runtime/tmplog/capture1_log_'+machine+'.log','w')
    capture1_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+capture1+'"', shell = True, stdout=capture1_log, stderr=capture1_log)
    sleep(0.1)
    capture2_log = open('/mnt/nfs/runtime/tmplog/capture2_log_'+machine+'.log','w')
    capture2_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+capture2+'"', shell = True, stdout=capture2_log, stderr=capture2_log)
    sleep(0.1)
    capture3_log = open('/mnt/nfs/runtime/tmplog/capture3_log_'+machine+'.log','w')
    capture3_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+capture3+'"', shell = True, stdout=capture3_log, stderr=capture3_log)
    sleep(0.1)
    capture4_log = open('/mnt/nfs/runtime/tmplog/capture4_log_'+machine+'.log','w')
    capture4_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+capture4+'"', shell = True, stdout=capture4_log, stderr=capture4_log)
    sleep(0.1)
    capture5_log = open('/mnt/nfs/runtime/tmplog/capture5_log_'+machine+'.log','w')
    capture5_proc = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; '+capture5+'"', shell = True, stdout=capture5_log, stderr=capture5_log)
    sleep(0.1)
    
    
if stop:

    print 'Killing everything'

    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q dbnull"',shell=True)
    subprocess.Popen.wait(output0)
    
    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q dsaX_filflag"',shell=True)
    subprocess.Popen.wait(output0)

    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q dsaX_dbmerge"',shell=True)
    subprocess.Popen.wait(output0)

    output0 = subprocess.Popen('ssh user@'+machine+' "source ~/.bashrc; killall -q dsaX_spectrometer_udpdb_thread"',shell=True)
    subprocess.Popen.wait(output0)

