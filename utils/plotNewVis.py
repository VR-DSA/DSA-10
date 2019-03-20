import numpy as np, pyfits as pf, matplotlib.pyplot as plt, pylab
from astropy.time import Time
from astropy import units as u
from astropy.stats import median_absolute_deviation as MAD


# will look at spectrum in blocks of tbin, and flag anything above/below median at thresh sigma
def cleanVis(fl=None,tbin=100,thresh=6.0,plot=True,apply=False,filename='flagged.fits'):

    if fl is None:
        print 'cleanVis(fl=None,tbin=100,thresh=6.0,plot=True,apply=False)'
        return 0
    
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = (f.header['NAXIS2'])
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    nchan = f.header['NCHAN']
    fch1 = f.header['FCH1']-(nchan*2-1)*250./2048.    
    t1 = 0
    t2 = nrow-1
    freqs = (np.arange(nchan)*(500./2048.)+fch1)

    ants = f.header['ANTENNAS'].split('-')
    bases = []
    for i in range(10):
        for j in range(i+1):
            bases.append(ants[i]+'-'+ants[j])
    
    data = np.flip(f.data['VIS'].reshape((nrow,55,nchan,2,2)),axis=2)
    tmax = np.floor((t2-t1)/(tbin*1.)).astype('int')*tbin
    tims = np.arange(nrow)*tsamp
    tims = tims[0:tmax]
    bindata = (data[0:tmax,:,:,:,:]).reshape((tmax/tbin,tbin,55,nchan,2,2)).mean(axis=1)
    bindata = 5.*(np.log10(bindata[:,:,:,:,0]**2.+bindata[:,:,:,:,1]**2.))
    linbindata = 10.**(bindata/5.)

    bins = bindata.shape[0]
    flags = np.zeros((55,nchan,2))+1.
    for tmbin in range(bins):

        flags *= 0.

        for bl in range(55):
            for pol in range(2):

                stddev = 1.4826*MAD(bindata[tmbin,bl,:,pol])
                med = np.median(bindata[tmbin,bl,:,pol])
                wrs = np.where(np.abs(bindata[tmbin,bl,:,:]-med)>thresh*stddev)
                flags[bl,wrs,pol] = 1.
    
        print 'TBIN ',tmbin,' of ',bins,': flagging percent ',np.sum(flags)*100./(1.*np.size(flags))

        if plot is True:
            plt.ion()
            
            for pl in range(55):
                pylab.subplot(11,5,pl+1)
                plt.plot(freqs,bindata[tmbin,pl,:,0],'r-')
                plt.plot(freqs,bindata[tmbin,pl,:,1],'b-')

                plcr= 5.*np.log10(linbindata[tmbin,pl,:,0]*flags[pl,:,0])
                plt.plot(freqs,plcr,'rx')
                plcr= 5.*np.log10(linbindata[tmbin,pl,:,1]*flags[pl,:,1])
                plt.plot(freqs,plcr,'bx')
                plt.title(bases[pl])
                

        if apply is True:
            flags -= 1.
            flags *= -1.
            data[tmbin*tbin:(tmbin+1)*tbin,:,:,:,0] *= flags
            data[tmbin*tbin:(tmbin+1)*tbin,:,:,:,1] *= flags

        a = raw_input('Any key to continue')
        if plot is True:
            plt.close()

    if apply is True:
        fout = pf.open(fl,ignore_missing_end=True)
        data = np.flip(data,axis=2)
        fout[1].data['VIS'] = data.ravel().reshape((nrow,data.size/nrow))
        fout.writeto(filename,clobber=True)
       
            
                
                
def mergeVis(fls=None,filename='merged.fits'):

    if fls is None:
        print 'Need list of file names as fls'
        print 'Assumes dsa-10 normal format, and coeval files'
        print 'looks for freqs 1487.27539 1456.75781 1426.24023 1395.72266 1365.20508'
        return 0

    # sort out header info
    nfls = len(fls)
    fout = pf.open(fls[0],ignore_missing_end=True)
    out_head = fout[1].header
    tsamp = fout[1].header['TSAMP']
    antennas = fout[1].header['ANTENNAS']
    
    smjd = fout[1].header['MJD']
    # find latest start mjd and
    for fl in fls:
        ff = pf.open(fl,ignore_missing_end=True)[1]
        if ff.header['MJD']>smjd:
            smjd = ff.header['MJD']

    # find actual number of samples
    min_nrow = fout[1].header['NAXIS2']
    for fl in fls:
        ff = pf.open(fl,ignore_missing_end=True)[1]
        nrow = ff.header['NAXIS2']
        mjd = ff.header['MJD']
        skipsamps = int(np.floor((smjd-mjd)*86400./tsamp))
        if nrow-skipsamps<min_nrow:
            min_nrow = nrow-skipsamps
        
    output_data = np.zeros((min_nrow,55,625,2,2),dtype=np.float32)
    for fl in fls:

        ff = pf.open(fl,ignore_missing_end=True)[1]
        print ff.header['FCH1']
        if np.abs(ff.header['FCH1']-1487.27539)<0.1:
            fi = 0
        if np.abs(ff.header['FCH1']-1456.75781)<0.1:
            fi = 1
        if np.abs(ff.header['FCH1']-1426.24023)<0.1:
            fi = 2
        if np.abs(ff.header['FCH1']-1395.72266)<0.1:
            fi = 3
        if np.abs(ff.header['FCH1']-1365.20508)<0.1:
            fi = 4

        mjd = ff.header['MJD']
        skipsamps = int(np.floor((smjd-mjd)*86400./tsamp))
        dat = (np.reshape(ff.data['VIS'],(nrow,55,125,2,2)))[skipsamps:skipsamps+min_nrow,:,:,:,:]
            
        output_data[:,:,fi*125:(fi+1)*125,:,:] = dat

    output_data = np.ravel(output_data).reshape((min_nrow,27500*5))

    # make new pyfits extension
    col1 = pf.Column(name='VIS',format='137500E',array=output_data)
    cols = pf.ColDefs([col1])
    tbhdu = pf.BinTableHDU.from_columns(cols)
    tbhdu.header.set('FCH1',1487.27539)
    tbhdu.header.set('NCHAN',625)
    tbhdu.header.set('MJD',smjd)
    tbhdu.header.set('TSAMP',tsamp)
    tbhdu.header.set('ANTENNAS',antennas)

    # write
    final_list = pf.HDUList([fout[0],tbhdu])
    final_list.writeto(filename,clobber=True)
    
    
    

def printInfo(fl=None):

    if fl is None:
        fl = '/mnt/nfs/data/dsatest.fits'
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = (f.header['NAXIS2'])
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    fch1 = f.header['FCH1']
    nchan = f.header['NCHAN']

    tstart = Time(mjd,format='mjd')-7.*u.hour
    tend = Time(mjd+nrow*tsamp/86400.,format='mjd')-7.*u.hour
    print 'Time interval:',tstart.iso,'to',tend.iso
    print '(start MJD ',mjd,')'
    print 'Freq range:',fch1,fch1-nchan*2.*250./2048
    print 'Antenna order:',(f.header['ANTENNAS']).split('-')
    print 'NCHANS:',nchan
    print 'TSAMP:',tsamp

    
def plotDelay(fl=None,tmid=None,tspan=None,f1=None,f2=None):

    if fl is None:
        fl = '/mnt/nfs/data/dsatest.fits'
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = int((f.header['NAXIS2']))
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    nchan = f.header['NCHAN']
    fch1 = f.header['FCH1']-(nchan*2-1)*250./2048.
    if1 = 0
    if2 = nchan
    if f1 is not None:
        if f2 is not None:
            if1 = np.floor((-fch1+f1)/(500./2048.)).astype('int')
            if2 = np.floor((-fch1+f2)/(500./2048.)).astype('int')
            
    if tmid is None:
        t1 = 0
        t2 = nrow-1
    else:
        time = (Time(tmid)-(-7.*u.hour)).mjd
        t1 = np.floor(((time-mjd)*24.*3600.-tspan/2.)/tsamp).astype('int')
        t2 = np.floor(((time-mjd)*24.*3600.+tspan/2.)/tsamp).astype('int')

    data = np.flip(f.data['VIS'].reshape((nrow,55,nchan,2,2)),axis=2)[t1:t2,:,if1:if2,:,:]
    data = data.mean(axis=0)
    freqs = (np.arange(nchan)*(500./2048.)+fch1)[if1:if2]

    d = data[:,:,:,0]+1j*data[:,:,:,1]
    x = 1e6*(np.arange((if2-if1))-(if2-if1)/2.)*2./(2.5e8*250./2048.)
    
    ants = f.header['ANTENNAS'].split('-')
    bases = []
    for i in range(10):
        for j in range(i+1):
            bases.append(ants[i]+'-'+ants[j])
    
    plt.ion()
    for pl in range(55):

        pylab.subplot(11,5,pl+1)
        y = np.fft.fftshift(np.abs(np.fft.ifft(d[pl,:,0]))**2.)
        y -= np.median(y)
        plt.plot(x,y,'r-')
        y = np.fft.fftshift(np.abs(np.fft.ifft(d[pl,:,1]))**2.)
        y -= np.median(y)
        plt.plot(x,y,'b-')
        plt.title(bases[pl])


def plotTimeAuto(fl=None,tmid=None,tspan=None,f1=None,f2=None,tbin=1):

    if fl is None:
        fl = '/mnt/nfs/data/dsatest.fits'
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = (f.header['NAXIS2'])
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    nchan = f.header['NCHAN']
    fch1 = f.header['FCH1']-(nchan*2-1)*250./2048.
    if1 = 0
    if2 = nchan
    if f1 is not None:
        if f2 is not None:
            if1 = np.floor((-fch1+f1)/(500./2048.)).astype('int')
            if2 = np.floor((-fch1+f2)/(500./2048.)).astype('int')

    if tmid is None:
        t1 = 0
        t2 = nrow-1
    else:
        time = (Time(tmid)-(-7.*u.hour)).mjd
        t1 = np.floor(((time-mjd)*24.*3600.-tspan/2.)/tsamp).astype('int')
        t2 = np.floor(((time-mjd)*24.*3600.+tspan/2.)/tsamp).astype('int')

    data = np.flip(f.data['VIS'].reshape((nrow,55,nchan,2,2)),axis=2)[t1:t2,:,if1:if2,:,:].mean(axis=2)
    tmax = np.floor((t2-t1)/(tbin*1.)).astype('int')*tbin
    tims = np.arange(nrow)*tsamp
    tims = tims[0:tmax]
    data = data[0:tmax,:,:,:]

    tims = tims.reshape((tmax/tbin,tbin)).mean(axis=1)
    data = data.reshape((tmax/tbin,tbin,55,2,2)).mean(axis=1)
    amps = 5.*(np.log10(data[:,:,:,0]**2.+data[:,:,:,1]**2.))

    ants = f.header['ANTENNAS'].split('-')
    bases = []
    autoy = []
    for i in range(10):
        for j in range(i+1):
            bases.append(ants[i]+'-'+ants[j])
            if i==j:
                autoy.append(1)
            else:
                autoy.append(0)
            

    # fix up tims
    tims = tims-np.mean(tims)
    tims = tims*15./(3600.)

                
    plt.ion()
    minn = 0.
    for pl in range(55):

        if autoy[pl]==1:
            #plt.plot(tims,np.sqrt(amps[:,pl,0]**2.+amps[:,pl,1]**2.),label=bases[pl])
            y = amps[:,pl,0]
            y -= np.max(y)
            if np.min(y)<minn:
                minn = np.min(y)
            plt.plot(tims,y,label=bases[pl])

            print 'MAX of',bases[pl],'at',tims[y==np.max(y)],'deg'
            xx = np.zeros(1)+tims[y==np.max(y)]
            yy = np.zeros(1)+np.max(y)
            plt.plot(xx,yy,'ko')
            
    xx = np.zeros(2)+np.mean(tims)
    yy = np.zeros(2)
    yy[0] = minn
    yy[1] = 0.
    plt.plot(xx,yy,'--')
    plt.legend()
    #return (tims,amps)
    
# tmid as a standard astropy time, tspan in seconds
def plotTimeAmp(fl=None,tmid=None,tspan=None,f1=None,f2=None,tbin=1):

    if fl is None:
        fl = '/mnt/nfs/data/dsatest.fits'
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = (f.header['NAXIS2'])
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    nchan = f.header['NCHAN']
    fch1 = f.header['FCH1']-(nchan*2-1)*250./2048.
    if1 = 0
    if2 = nchan
    if f1 is not None:
        if f2 is not None:
            if1 = np.floor((-fch1+f1)/(500./2048.)).astype('int')
            if2 = np.floor((-fch1+f2)/(500./2048.)).astype('int')

    if tmid is None:
        t1 = 0
        t2 = nrow-1
    else:
        time = (Time(tmid)-(-7.*u.hour)).mjd
        t1 = np.floor(((time-mjd)*24.*3600.-tspan/2.)/tsamp).astype('int')
        t2 = np.floor(((time-mjd)*24.*3600.+tspan/2.)/tsamp).astype('int')

    data = np.flip(f.data['VIS'].reshape((nrow,55,nchan,2,2)),axis=2)[t1:t2,:,if1:if2,:,:].mean(axis=2)
    tmax = np.floor((t2-t1)/(tbin*1.)).astype('int')*tbin
    tims = np.arange(nrow)*tsamp
    tims = tims[0:tmax]
    data = data[0:tmax,:,:,:]

    tims = tims.reshape((tmax/tbin,tbin)).mean(axis=1)
    data = data.reshape((tmax/tbin,tbin,55,2,2)).mean(axis=1)
    amps = 5.*(np.log10(data[:,:,:,0]**2.+data[:,:,:,1]**2.))

    ants = f.header['ANTENNAS'].split('-')
    bases = []
    for i in range(10):
        for j in range(i+1):
            bases.append(ants[i]+'-'+ants[j])
    
    plt.ion()
    for pl in range(55):

        pylab.subplot(11,5,pl+1)
        plt.plot(tims,amps[:,pl,0],'r-')
        plt.plot(tims,amps[:,pl,1],'b-')
        plt.title(bases[pl])

    return (tims,amps)

# tmid as a standard astropy time, tspan in seconds
def plotTimePhase(fl=None,tmid=None,tspan=None,f1=None,f2=None,tbin=1):

    if fl is None:
        fl = '/mnt/nfs/data/dsatest.fits'
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = (f.header['NAXIS2'])
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    nchan = f.header['NCHAN']
    fch1 = f.header['FCH1']-(nchan*2-1)*250./2048.
    if1 = 0
    if2 = nchan
    if f1 is not None:
        if f2 is not None:
            if1 = np.floor((-fch1+f1)/(500./2048.)).astype('int')
            if2 = np.floor((-fch1+f2)/(500./2048.)).astype('int')

    if tmid is None:
        t1 = 0
        t2 = nrow-1
    else:
        time = (Time(tmid)-(-7.*u.hour)).mjd
        t1 = np.floor(((time-mjd)*24.*3600.-tspan/2.)/tsamp).astype('int')
        t2 = np.floor(((time-mjd)*24.*3600.+tspan/2.)/tsamp).astype('int')

    data = np.flip(f.data['VIS'].reshape((nrow,55,nchan,2,2)),axis=2)[t1:t2,:,if1:if2,:,:].mean(axis=2)
    tmax = np.floor((t2-t1)/(tbin*1.)).astype('int')*tbin
    tims = np.arange(nrow)*tsamp
    tims = tims[0:tmax]
    data = data[0:tmax,:,:,:]

    tims = tims.reshape((tmax/tbin,tbin)).mean(axis=1)
    data = data.reshape((tmax/tbin,tbin,55,2,2)).mean(axis=1)
    angs = (180./np.pi)*np.angle(data[:,:,:,0]+data[:,:,:,1]*1j)

    ants = f.header['ANTENNAS'].split('-')
    bases = []
    for i in range(10):
        for j in range(i+1):
            bases.append(ants[i]+'-'+ants[j])
    
    plt.ion()
    for pl in range(55):

        pylab.subplot(11,5,pl+1)
        plt.ylim(-180.,180.)
        plt.plot(tims,angs[:,pl,0],'r-')
        plt.plot(tims,angs[:,pl,1],'b-')
        plt.title(bases[pl])

    return (tims,angs)

# tmid as a standard astropy time, tspan in seconds
def plotFreqAmp(fl=None,tmid=None,tspan=None,f1=None,f2=None):

    if fl is None:
        fl = '/mnt/nfs/data/dsatest.fits'
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = int((f.header['NAXIS2']))
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    nchan = f.header['NCHAN']
    fch1 = f.header['FCH1']-(nchan*2-1)*250./2048.
    if1 = 0
    if2 = nchan
    if f1 is not None:
        if f2 is not None:
            if1 = np.floor((-fch1+f1)/(500./2048.)).astype('int')
            if2 = np.floor((-fch1+f2)/(500./2048.)).astype('int')
            
    if tmid is None:
        t1 = 0
        t2 = nrow-1
    else:
        time = (Time(tmid)-(-7.*u.hour)).mjd
        t1 = np.floor(((time-mjd)*24.*3600.-tspan/2.)/tsamp).astype('int')
        t2 = np.floor(((time-mjd)*24.*3600.+tspan/2.)/tsamp).astype('int')

    data = np.flip(f.data['VIS'].reshape((nrow,55,nchan,2,2)),axis=2)[t1:t2,:,if1:if2,:,:]
    data = data.mean(axis=0)
    freqs = (np.arange(nchan)*(500./2048.)+fch1)[if1:if2]
    
    amps = 5.*(np.log10(data[:,:,:,0]**2.+data[:,:,:,1]**2.))

    ants = f.header['ANTENNAS'].split('-')
    bases = []
    for i in range(10):
        for j in range(i+1):
            bases.append(ants[i]+'-'+ants[j])
    
    plt.ion()
    for pl in range(55):

        pylab.subplot(11,5,pl+1)
        plt.plot(freqs,amps[pl,:,0],'r-')
        plt.plot(freqs,amps[pl,:,1],'b-')
        plt.title(bases[pl])

    return (freqs,amps)

# tmid as a standard astropy time, tspan in seconds
def plotFreqPhase(fl=None,tmid=None,tspan=None,f1=None,f2=None):

    if fl is None:
        fl = '/mnt/nfs/data/dsatest.fits'
    f = pf.open(fl,ignore_missing_end=True)[1]
    nrow = (f.header['NAXIS2'])
    mjd = f.header['MJD']
    tsamp = f.header['TSAMP']
    nchan = f.header['NCHAN']
    fch1 = f.header['FCH1']-(nchan*2-1)*250./2048.
    if1 = 0
    if2 = nchan
    if f1 is not None:
        if f2 is not None:
            if1 = np.floor((-fch1+f1)/(500./2048.)).astype('int')
            if2 = np.floor((-fch1+f2)/(500./2048.)).astype('int')

    if tmid is None:
        t1 = 0
        t2 = nrow-1
    else:
        time = (Time(tmid)-(-7.*u.hour)).mjd
        t1 = np.floor(((time-mjd)*24.*3600.-tspan/2.)/tsamp).astype('int')
        t2 = np.floor(((time-mjd)*24.*3600.+tspan/2.)/tsamp).astype('int')

    data = np.flip(f.data['VIS'].reshape((nrow,55,nchan,2,2)),axis=2)[t1:t2,:,if1:if2,:,:]
    data = data.mean(axis=0)
    freqs = (np.arange(nchan)*(500./2048.)+fch1)[if1:if2]


    angs = (180./np.pi)*np.angle(data[:,:,:,0]+data[:,:,:,1]*1j)

    ants = f.header['ANTENNAS'].split('-')
    bases = []
    for i in range(10):
        for j in range(i+1):
            bases.append(ants[i]+'-'+ants[j])
    
    plt.ion()
    for pl in range(55):

        pylab.subplot(11,5,pl+1)
        plt.ylim(-180.,180.)
        plt.plot(freqs,angs[pl,:,0],'r-')
        plt.plot(freqs,angs[pl,:,1],'b-')
        plt.title(bases[pl])

    return (freqs,angs)
