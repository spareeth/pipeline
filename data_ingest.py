#!/usr/bin/python3
'''
Sajid Pareeth, 2023
The script does post processing of fewsnet data based on countries
Zonal stats to extract mean ndvi per cluster
'''

import os
import subprocess
import sys
import glob
import click
import uuid
import shutil
import grass_session
from grass_session import Session
from grass.pygrass.modules.shortcuts import general as g
from grass.pygrass.modules.shortcuts import raster as r
from grass.pygrass.modules.shortcuts import display as d
from grass.pygrass.modules.shortcuts import vector as v
from grass.pygrass.gis import *
import grass.script as grass
import grass.script.setup as gsetup
from pathlib import Path
import zipfile
from sh import gunzip

#data='viirs_ndvi'
#inputfolder='/home/ubuntu/data/fewsnet'

@click.command()
#General
@click.argument('data')
@click.argument('infolder')

def main(data, infolder):
    """Ingest the newly downloaded fewsnet data into map database.
        
        Arguments:
        
        data - Options are arc2_daily, chirps_monthly, tamsat_monthly, tamsat_daily, eandvi_viirs, eandviano_viirs, eandvi_modis, eandviano_modis, sandvi_viirs, sandviano_viirs, sandvi_modis, sandviano_modis

        infolder - Add the full path to the folder where list of downloaded zip files are stored

    """
    try:
        ## USER INPUTS ##
        data = r"%s" %str(data)
        INDAT = r"%s" %str(infolder)
        gisdb = '/home/ubuntu/s3-mount/mapdata'
        location = 'latlong'
        if data == 'eandvi_viirs':
            mapset = 'eandvi_viirs'
        elif data == 'eandviano_viirs':
            mapset = 'eandviano_viirs'
        elif data == 'sandvi_viirs':
            mapset = 'sandvi_viirs'
        elif data == 'sandviano_viirs':
            mapset = 'sandviano_viirs'
        elif data == 'eandvi_modis':
            mapset = 'eandvi_modis'
        elif data == 'eandviano_modis':
            mapset = 'eandviano_modis'
        elif data == 'sandvi_modis':
            mapset = 'sandvi_modis'
        elif data == 'sandviano_modis':
            mapset = 'sandviano_modis'
        elif data == 'tamsat_monthly':
            mapset = 'tamsat_monthly'
        elif data == 'tamsat_daily':
            mapset = 'tamsat_daily'
        elif data == 'chirps_monthly':
            mapset = 'chirps_monthly'
        elif data == 'arc2_daily':
            mapset = 'arc2_daily'
        else:
            print('data is not supported')
            return

        print(mapset)
        # Python path: we ask GRASS GIS where its Python packages are
        sys.path.append(
            subprocess.check_output(["grass", "--config", "python_path"], text=True).strip()
        )

        # set some common environmental variables, like:
        os.environ.update(dict(GRASS_COMPRESS_NULLS='1',
                                GRASS_OVERWRITE='1',
                                GRASS_COMPRESSOR='ZSTD'))

        #user = Session()
        #user.open(gisdb=gisdb, location=location, mapset=mapset)
        session = gsetup.init(gisdb, location, mapset)

        if data == 'eandvi_viirs' or data == 'eandviano_viirs' or data == 'sandvi_viirs' or data == 'sandviano_viirs':
            s1="*.zip"
            pt1=os.path.join(INDAT, s1)
            listzip=glob.glob(pt1)
            print(listzip)
            for dt1 in listzip:
                with zipfile.ZipFile(dt1, 'r') as zip_ref:
                    zip_ref.extractall(INDAT)
            print('Unzipping files')

            s2="*.tif"
            pt2=os.path.join(INDAT, s2)
            listtif=glob.glob(pt2)

            for dt2 in listtif:
                out1=os.path.basename(dt2)
                #out2=out1.rsplit('.',1)[0]
                yr = out1.rsplit('.',1)[0][2:][:2]
                yy = '20'+yr
                dt = out1.rsplit('.',1)[0][4:][:2]
                out = mapset + '_' + yy + '_' + dt
                r.in_gdal(input=dt2, output=out, overwrite=True)
                ##grass.run_command("r.in.gdal", input=dt, output=out2, overwrite=True)
        else:
            print('data is rainfall')

        ## Data source for tamsat:
        ## wget https://gws-access.jasmin.ac.uk/public/tamsat/rfe/data/v3.1/monthly/${i}/${m}/rfe${i}_${m}.v3.1.nc (i=year, m=month)
        if data == 'tamsat_monthly':
            s1="*.nc"
            pt1=os.path.join(INDAT, s1)
            listnc=glob.glob(pt1)
            for dt2 in listnc:
                out1=os.path.basename(dt2)
                yr = out1.rsplit('.',3)[0][3:][:4]
                mm = out1.rsplit('.',3)[0][8:][:9]
                in1 = 'NETCDF' + ':"' + INDAT + '/rfe' + yr + '_' + mm + '.v3.1.nc":rfe_filled'
                out = "tamsat" + '_rfe' + yr + '_' + mm
                r.in_gdal(input=in1, output=out, flags="o", overwrite=True)

        if data == 'tamsat_daily':
            s1="*.nc"
            pt1=os.path.join(INDAT, s1)
            listnc=glob.glob(pt1)
            for dt2 in listnc:
                out1=os.path.basename(dt2)
                yr = out1.rsplit('.',3)[0][3:][:4]
                mm = out1.rsplit('.',3)[0][8:10]
                dd = out1.rsplit('.',3)[0][11:14]
                in1 = 'NETCDF' + ':"' + INDAT + '/rfe' + yr + '_' + mm + '_'+ dd + '.v3.1.nc":rfe_filled'
                out = "tamsat" + '_rfe' + yr + '_' + mm + '_' + dd
                r.in_gdal(input=in1, output=out, flags="o", overwrite=True)
        
        ## Data source for Chirps - # Chirps monthly: https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/
        ## wget -r -np -R "index.html*" -e robots=off https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/
        ## chirps-v2.0.2023.06.tif
        if data == 'chirps_monthly':
            s1="*.gz"
            pt1=os.path.join(INDAT, s1)
            listnc=glob.glob(pt1)

            for dt2 in listnc:
                gunzip(dt2, "-f")
                out1=os.path.basename(dt2)
                yr = out1.rsplit('.',4)[1]
                mm = out1.rsplit('.',4)[2]
                in1 = out1.rsplit('.',1)[0]
                in2 = INDAT + "/" + in1
                out = 'chirps_monthly_' + yr + '_' + mm
                r.in_gdal(input=in2, output=out, flags="o", overwrite=True)

        ## ARC2 Precipitation data
        if data == 'arc2_daily':
            s1="*.zip"
            pt1=os.path.join(INDAT, s1)
            listzip=glob.glob(pt1)
            print(listzip)
            for dt1 in listzip:
                with zipfile.ZipFile(dt1, 'r') as zip_ref:
                    zip_ref.extractall(INDAT)
            print('Unzipping files')

            s2="*.tif"
            pt2=os.path.join(INDAT, s2)
            listtif=glob.glob(pt2)

            for dt2 in listtif:
                out1=os.path.basename(dt2)
                #out2=out1.rsplit('.',1)[0]
                dt = out1.rsplit('.',2)[1]
                out = mapset + '_' + dt
                r.in_gdal(input=dt2, output=out, overwrite=True)
                ##grass.run_command("r.in.gdal", input=dt, output=out2, overwrite=True)

        session.finish()
    finally:
        files = os.listdir(INDAT)

        for item in files:
            if item.endswith(".zip"):
                os.remove(os.path.join(INDAT, item))

        for item in files:
            if item.endswith(".tif"):
                os.remove(os.path.join(INDAT, item))

        for item in files:
            if item.endswith(".tfw"):
                os.remove(os.path.join(INDAT, item))
        
        for item in files:
            if item.endswith(".nc"):
                os.remove(os.path.join(INDAT, item))
if __name__ == '__main__':
    main()
