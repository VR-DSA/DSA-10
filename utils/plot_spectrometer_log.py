import numpy as np, matplotlib.pyplot as plt, time, matplotlib, sys, pyfits

fl = sys.argv[1]
tcyc = 2
plt.ion()
freqs = 1530.-np.arange(2048)*250./2048.

NSAMPS = 12500.
nant=5.
tsamp = 12500.*131.072e-6

while 1==1:

    try:

        data = pyfits.open(fl)[1].data
        clipped = data['Perc_samp']
        clipped_ts = data['Perc_ts']
        x = data['Spectra'][-2,:]
        specs = 10.*np.log10(np.abs(x))
        tx = np.arange(len(clipped))*tsamp
    
        s1 = specs[0:2048]
        nbad = len((np.where(x<0))[0])
    
        plt.subplot(211)
        plt.cla()
        plt.ylim(40.,1.2*s1.max())
        plt.plot(freqs,s1,'r-')
        plt.plot(freqs[np.where(x[0:2048]<0)],s1[np.where(x[0:2048]<0)],'rx',label='S1')
        plt.legend()
        plt.xlabel('Frequency (MHz)')
        plt.ylabel('Relative dB')
        plt.title('Number of bad channels: '+str(nbad))
        plt.pause(0.1)
        
        plt.subplot(413)
        plt.cla()
        plt.plot(tx,clipped)
        plt.xlabel('Time (s)')
        plt.ylabel('Percentage of clipped samples')
        plt.pause(0.1)
    
        plt.subplot(414)
        plt.cla()
        plt.plot(tx,clipped_ts)
        plt.xlabel('Time (s)')
        plt.ylabel('Percentage of clipped zero-DM timeseries')
        plt.pause(0.1)
        
    except:

        print 'Cannot open file at the moment'
        

    print 'Sleeping'
    time.sleep(tcyc)
