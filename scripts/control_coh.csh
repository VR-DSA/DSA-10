#!/bin/csh
#

if (${1} == 'create') then

    python setup_coh.py dsamaster create
    python setup_coh.py dsa1 create
    python setup_coh.py dsa2 create
    python setup_coh.py dsa3 create
    python setup_coh.py dsa4 create
    #python setup_coh_dsa5.py create

endif

if (${1} == 'start') then

    #python setup_coh_dsa5.py start
    python setup_coh.py dsamaster start_rx
    python setup_coh.py dsa1 start_rx
    python setup_coh.py dsa2 start_rx
    python setup_coh.py dsa3 start_rx
    python setup_coh.py dsa4 start_rx
    sleep 1
    python setup_coh.py dsamaster start_tx
    python setup_coh.py dsa1 start_tx
    python setup_coh.py dsa2 start_tx
    python setup_coh.py dsa3 start_tx
    python setup_coh.py dsa4 start_tx

endif

if (${1} == 'stop') then

    python setup_coh.py dsamaster stop
    python setup_coh.py dsa1 stop
    python setup_coh.py dsa2 stop
    python setup_coh.py dsa3 stop
    python setup_coh.py dsa4 stop
    #python setup_coh_dsa5.py stop

endif

if (${1} == 'destroy') then

    python setup_coh.py dsamaster destroy
    python setup_coh.py dsa1 destroy
    python setup_coh.py dsa2 destroy
    python setup_coh.py dsa3 destroy
    python setup_coh.py dsa4 destroy
    #python setup_coh_dsa5.py destroy

endif


if (${1} == "clean") then

    

    echo "Archiving on all machines"
    ssh user@dsa1 "source ~/.bashrc; /mnt/nfs/code/dsaX/cohsrc/final_archive.bash dsa1"
    ssh user@dsa2 "source ~/.bashrc; /mnt/nfs/code/dsaX/cohsrc/final_archive.bash dsa2"
    ssh user@dsa3 "source ~/.bashrc; /mnt/nfs/code/dsaX/cohsrc/final_archive.bash dsa3"
    ssh user@dsa4 "source ~/.bashrc; /mnt/nfs/code/dsaX/cohsrc/final_archive.bash dsa4"
    /mnt/nfs/code/dsaX/cohsrc/final_archive.bash dsamaster

    echo "Cleaning heimdall dir, and raw dumps"
    
    rm -rf /mnt/nfs/data/heimdall/heimdall.cand
    rm -rf /mnt/nfs/data/heimdall/*.filt
    rm -rf /mnt/nfs/website/IMGS/*.png
    ssh user@dsa1 "source ~/.bashrc; rm -rf /home/user/data/raw/*.out"
    ssh user@dsa2 "source ~/.bashrc; rm -rf /home/user/data/raw/*.out"
    ssh user@dsa3 "source ~/.bashrc; rm -rf /home/user/data/raw/*.out"
    ssh user@dsa4 "source ~/.bashrc; rm -rf /home/user/data/raw/*.out"
    rm -rf /home/user/data/raw/*.out
    
    echo "Archiving cal and cand plots"
    set mjd=`sed -n 1p /mnt/nfs/runtime/UTC_START.txt`
    scp -r /mnt/nfs/website/CANDS dsa-storage:/mnt/data/images/${mjd}
    #scp -r /mnt/nfs/website/calibrations dsa-storage:/mnt/data/calibrations/${mjd}
    rm -rf /mnt/nfs/website/CANDS/*
    #rm -rf /mnt/nfs/website/calibrations/*
    ssh dsa-storage "source ~/.bashrc; rm -rf /var/www/html/CANDS/*"
    #ssh dsa-storage "source ~/.bashrc; rm -rf /var/www/html/calibrations/*"
        
endif
