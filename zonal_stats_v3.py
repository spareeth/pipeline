# -*- coding: utf-8 -*-
#!/usr/bin/env python
'''
Sajid Pareeth, 2023
The script does post processing of fewsnet data based on countries
Zonal stats to extract mean ndvi per cluster
'''

import os
import os.path
import subprocess
import sys
import glob
from turtle import st
import uuid
import datetime
from datetime import datetime
import shutil
import subprocess
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
import click

## USER INPUTS ##
#infile='/home/ubuntu/data/boundaries/sdn_bnd_admin1_a_gov.shp'
#data='viirs_ndvi'
#start=2023
#end=2023

@click.command()
#General
@click.argument('infile')
@click.argument('data')
@click.argument('method')
@click.argument('start', type=int)
@click.argument('end', type=int)

def main(infile, data, method, start, end):
    """Ingest the newly downloaded fewsnet data into map database.
      
        Arguments:
        
        infile - Add the full path to the shapefile (only the file with extension '.shp'

        data - Options are viirs_ndvi, viirs_ndviano, modis_ndvi, modis_ndviano

        method - statistical metric to be computed - average, minimum, maximum, stddev, median

        start - start year

        end - end year 
    """
    infile = r"%s" %str(infile)
    data = r"%s" %str(data)
    method = r"%s" %str(method)
    #start = r"%s" %str(start)
    #end = r"%s" %str(end)

    basepath=os.path.dirname(infile)
    gisdb='/home/ubuntu/efs-mount/mapdata'
    location='latlong'
    if data == 'viirs_ndvi':
        mapset = 'ndvi_viirs'
        p1 = ''
    elif data == 'viirs_ndviano':
        mapset = 'ndviano_viirs'
        p1 = ''
    elif data == 'modis_ndvi':
        mapset = 'ndvi_modis'
        p1 = 'patch'
    else:
        mapset = 'ndviano_modis'
        p1 = ''

    print(mapset)

    indat='/home/ubuntu/efs-mount/.temp'
    ##USER INPUTS FINISH HERE ###
    jobid=str(uuid.uuid4())
    # Python path: we ask GRASS GIS where its Python packages are
    sys.path.append(
        subprocess.check_output(["grass", "--config", "python_path"], text=True).strip()
    )

    # set some common environmental variables, like:
    os.environ.update(dict(GRASS_COMPRESS_NULLS='1',
                        GRASS_OVERWRITE='1',
                        GRASS_COMPRESSOR='ZSTD'))

    user = Session()
    user.open(gisdb=gisdb, location=location, mapset=jobid,
                create_opts='')

    g.mapsets(mapset=mapset, operation="add")
    timerange = range(int(start),int(end)+1)
    years = list(timerange)
    years_str = [str(s) for s in years]

    grass.run_command("v.import", input=infile, output="bnds", overwrite=True)
    #maps = ["ndvi_annual_" + s for s in years_str]
    g.region(vector="bnds", res=0.003)

    r.mask(raster="worldcover_sudan", maskcats='20 30')

    filename = str(uuid.uuid4())
    tmp=os.path.join(indat, filename)

    #with open(tmp, "a") as tmpfile:
    for yr in years_str:
        pattern=mapset + "_" + yr + "_*" + p1
        out=tmp + "_" + yr + ".txt"
        grass.run_command("g.list", type="rast", pattern=pattern, map=mapset, output=out, overwrite=True)

    s1=filename+"*.txt"
    pt2=os.path.join(indat, s1)
    list1=glob.glob(pt2)

    out1=os.path.join(indat, "mergednames")
    with open(out1, 'w') as outfile:
        for fname in list1:
            with open(fname) as infile:
                outfile.write(infile.read())

    with open(out1) as tmpfile:
        for line in tmpfile:
            in2=line.strip('\n')
            v.rast_stats(map='bnds', raster=in2, column_prefix=in2, method=method)

    r.mask(flags="r")

    now = datetime.now()
    a = int(now.strftime('%Y%m%d%H%M%S'))

    outfile="zonalstats_" + str(a) + ".csv"
    outfile1=os.path.join(basepath, outfile)
    v.out_ogr(input='bnds', output=outfile1, format='CSV')
    user.close()

    #####CLEANUP
    indatfiles = indat + "/*"
    files = glob.glob(indatfiles)
    for f in files:
        os.remove(f)

    locpth=os.path.join(gisdb, location, jobid)

    if os.path.exists(locpth) and os.path.isdir(locpth):
        shutil.rmtree(locpth)

if __name__ == '__main__':
    main()