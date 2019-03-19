#!/usr/bin/env python
# coding: utf-8

# In[1]:


import astropy.units as u
from astropy.time import Time
from astropy.coordinates import SkyCoord, EarthLocation, AltAz, Angle
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


# In[16]:


def get_alt_az_at_meridian(ra,dec, site,year,month,day):
    """Compute the altitude and azimuth of a source at its maximum elevation for a particular site on a particular day.
    ra is the source's right ascention, something that astropy's SkyCoord function will accept
    dec is the source's declination, something that astropy's SkyCoord function will accept
    site is an EarthLocation object of where your observatory is
    year is the year when you want to compute this function
    month is the month when you want to compute this function
    day is the day when you want to compute this function
    Returns:
    (alt, az)
    alt: the altitude of the source at meridian
    az: the azimuth of the source at meridian"""
    
    source = SkyCoord(ra, dec, frame='icrs')
    start_time = dt.datetime(year=year, month=month, day=day, hour=0, minute=0)
    times = np.array([start_time + dt.timedelta(minutes=i) for i in xrange(1440)])
    altaz_FRB = source.transform_to(AltAz(obstime=times,location=site))
    altitude_at_meridian = np.max(altaz_FRB.alt)
    meridian_index = np.where(altaz_FRB.alt == altitude_at_meridian)[0]
    azimuth_at_meridian = altaz_FRB.az[meridian_index[0]]
    return altitude_at_meridian.degree, azimuth_at_meridian.degree

def get_altaz_of_calibrators(calibrators_file,site):
    """Inputs:
    calibrators_file: a list of accepatble calibrator sources from NVSS for a particular source
    site is an EarthLocation object of where your observatory is
    Outputs:
    altaz_srcs: an array of SkyCoord objects with altitude and azimuth at the current time at the specified observatory"""
    
    calibrators = pd.read_csv(calibrators_file,delimiter='|',skiprows=2)
    calibrators.iloc[:,2] +=' hours'
    calibrators.iloc[:,3] +=' degrees'
    srcs = SkyCoord(calibrators.iloc[:,2],calibrators.iloc[:,3], frame='icrs')
    time_now = dt.datetime.now()
    altaz_srcs = srcs.transform_to(AltAz(obstime=time_now,location=site))
    return altaz_srcs

def get_calibrators_in_beam(sources,pointing_az,pointing_alt,time,site,tolerance):
    """Get the in-beam calibrators and return their SkyCoord objects.
    Inputs:
    sources: array of SkyCoord objects with altitude & azimuth computed
    pointing_az: the azimuth you are pointing to
    pointing_alt: the altitude you are pointing to
    time: the time for which to perform this calculation
    site: an EarthLocation object of where your observatory is
    Outputs:
    good_calibrators: An array of SkyCoord objects of calibrators that are in-beam"""

    pointing = AltAz(obstime=time_now,az = pointing_az*u.degree,alt=pointing_alt*u.degree,location=site)
    calibrator_separations = pointing.separation(sources)
    calibrators_in_beam = np.where(calibrator_separations.degree < tolerance)[0]
    good_calibrators = sources[calibrators_in_beam]
    return good_calibrators

def can_we_calibrate(ra, dec, latitude, longitude, elevation, pointing_year, pointing_month, pointing_day, calibrators_file,time, tolerance):
    """A control function that takes in bare bones inputs and spits out the calibrators, if any, that are in view.
    ra: the source's right ascention, something that astropy's SkyCoord function will accept
    dec: the source's declination, something that astropy's SkyCoord function will accept
    latitude: Astropy units object of the latitude of the observatory
    longitude: Astropy units object of the longitude of the observatory
    elevation: Astropy units object of the elevation of the observatory
    pointing_year: year the pointing was set
    pointing_month: month the pointing was set
    pointing_day: day the pointing was set
    calibrators_file: a list of accepatble calibrator sources from NVSS for a particular source
    time: time for which to perform the calculation
    
    """
    site = EarthLocation(lat = latitude, lon = longitude, height = elevation)
    meridian_alt, meridian_az = get_alt_az_at_meridian(ra = ra,dec = dec, site = site,year = pointing_year,month = pointing_month,day = pointing_day)
    calibrators_altaz = get_altaz_of_calibrators(calibrators_file = calibrators_file, site = site)
    good_calibrators = get_calibrators_in_beam(sources=calibrators_altaz,pointing_az=meridian_az,pointing_alt=meridian_alt,time=time,site=site, tolerance=tolerance)
    return good_calibrators


# In[17]:


if __name__ == '__main__':
    ra = '04h22m22s'
    dec = '+73d40m00s'
    latitude = 27.233247 * u.deg
    longitude = -118.283396 * u.deg
    elevation = 1222 * u.m
    pointing_year = 2019
    pointing_month = 4
    pointing_day = 1
    #calibrators_file = "C:/Users/boche/Documents/Work/DSA/calibrators/frb_calibrators.txt"
    calibrators_file = "frb_calibrators.txt"
    time_now = dt.datetime.now()
    tolerance = 3.5/np.cos(Angle(dec))
    good_calibrators = can_we_calibrate(ra,dec,latitude,longitude,elevation,pointing_year,pointing_month,pointing_day,calibrators_file,time_now,tolerance)
    if len(good_calibrators) > 0:
        print "Time to calibrate!"


# In[103]:





# In[97]:





# In[90]:





# In[65]:





# In[ ]:




