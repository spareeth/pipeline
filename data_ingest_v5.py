# -*- coding: utf-8 -*-
#!/usr/bin/env python3
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

#data='viirs_ndvi'
#inputfolder='/home/ubuntu/data/fewsnet'

@click.command()
#General
@click.argument('data')
@click.argument('infolder')
def main(data, infolder):
    """Ingest the newly downloaded fewsnet data into map database.
        
        Arguments:
        
        data - Options are viirs_ndvi, viirs_ndviano

        inputfolder - Add the full path to the folder were list of downloaded zip files are stored
        
    """
    ## USER INPUTS ##
    data = r"%s" %str(data)
    gisdb = '/home/ubuntu/efs-mount/mapdata'
    location = 'latlong'
    if data == 'viirs_ndvi':
        mapset = 'ndvi_viirs'
    elif data == 'viirs_ndviano':
        mapset = 'ndviano_viirs'
    elif data == 'modis_ndvi':
        mapset = 'ndvi_modis'
    else:
        mapset = 'ndviano_modis'

    print(mapset)
    INDAT = r"%s" %str(infolder)
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

    s1="*.zip"
    pt1=os.path.join(INDAT, s1)
    listzip=glob.glob(pt1)
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
        dt = out1.rsplit('.',1)[0][4:][:4]
        out = mapset + '_' + yy + '_' + dt
        r.in_gdal(input=dt2, output=out, overwrite=True)
        ##grass.run_command("r.in.gdal", input=dt, output=out2, overwrite=True)

    session.finish()
    files = os.listdir(INDAT)

    for item in files:
        if item.endswith(".tif"):
            os.remove(os.path.join(INDAT, item))

    for item in files:
        if item.endswith(".tfw"):
            os.remove(os.path.join(INDAT, item))

if __name__ == '__main__':
    main()
