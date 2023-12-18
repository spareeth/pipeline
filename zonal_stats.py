#!/usr/bin/python3
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
@click.option('--sh', is_flag=True, help="If true, will apply Land use mask on shrubland")
@click.option('--gr', is_flag=True, help="If true, will apply Land use mask on grassland")
@click.option('--cr', is_flag=True, help="If true, will apply Land use mask on cropland")
@click.option('--sg', is_flag=True, help="If true, will apply Land use mask on shrubland & grassland")
@click.argument('bnd')
@click.argument('data')
@click.argument('method')
@click.argument('outfolder')
@click.argument('outcsv')
@click.argument('start', type=int)
@click.argument('end', type=int)

def main(sh, gr, cr, sg, bnd, data, method, outfolder, outcsv, start, end):
    """ Extract statistics based on a vector boundary from raster time series
      
        Arguments:
        
        bnd - Name of the boundary for which statistics needed (use listing of vector boundaries to see the available boundaries)

        data - Options are arc2_daily, chirps_monthly, tamsat_monthly, tamsat_daily, eandvi_viirs, eandviano_viirs, eandvi_modis, eandviano_modis, sandvi_viirs, sandviano_viirs, sandvi_modis, sandviano_modis

        method - statistical metric to be computed - average, minimum, maximum, stddev, median

        outfolder - Add the full path to the folder where extracted csv will be stored

        outcsv - output csv file name (string, without extension)

        start - start year

        end - end year 
    """
    try:
        bnd = r"%s" %str(bnd)
        data = r"%s" %str(data)
        method = r"%s" %str(method)
        outfolder = r"%s" %str(outfolder)
        outcsv = r"%s" %str(outcsv)
        #start = r"%s" %str(start)
        #end = r"%s" %str(end)

        #basepath=os.path.dirname(infile)
        gisdb='/home/ubuntu/s3-mount/mapdata'
        location='latlong'
        if data == 'eandvi_viirs':
            mapset = 'eandvi_viirs'
            pat = 'ndvi_viirs'
            p1 = ''
        elif data == 'eandviano_viirs':
            mapset = 'eandviano_viirs'
            pat = 'ndviano_viirs'
            p1 = ''
        elif data == 'sandvi_viirs':
            mapset = 'sandvi_viirs'
            pat = 'sandvi_viirs'
            p1 = ''
        elif data == 'sandviano_viirs':
            mapset = 'sandviano_viirs'
            pat = 'sandviano_viirs'
            p1 = ''
        elif data == 'eandvi_modis':
            mapset = 'eandvi_modis'
            pat = 'ndvi_modis'
            p1 = 'corr'
        elif data == 'eandviano_modis':
            mapset = 'eandviano_modis'
            pat = 'ndviano_modis'
            p1 = ''
        elif data == 'sandvi_modis':
            mapset = 'sandvi_modis'
            pat = 'sandvi_modis'
            p1 = 'corr'
        elif data == 'sandviano_modis':
            mapset = 'sandviano_modis'
            pat = 'sandviano_modis'
            p1 = ''
        elif data == 'chirps_monthly':
            mapset = 'chirps_monthly'
            p1 = ''
        elif data == 'tamsat_monthly':
            mapset = 'tamsat_monthly'
            p1 = ''
        elif data == 'tamsat_daily':
            mapset = 'tamsat_daily'
            p1 = ''
        elif data == 'arc2_daily':
            mapset = 'arc2_daily'
            pat = 'arc2'
            p1 = ''
        else:
            print('data is not supported')
            return

        print(mapset)

        indat='/home/ubuntu/s3-mount/.temp'
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

        #grass.run_command("v.import", input=infile, output="bnds", overwrite=True)
        #maps = ["ndvi_annual_" + s for s in years_str]
        g.region(vector=bnd, res=0.002)
        grass.run_command('g.copy', vector=(bnd,'bnd'))

        if sh:
            r.mask(raster="worldcover", maskcats='20')
        elif gr:
            r.mask(raster="worldcover", maskcats='30')
        elif cr:
            r.mask(raster="worldcover", maskcats='40')
        elif sg:
            r.mask(raster="worldcover", maskcats='20 30')
        else:
            print('No mask')

        filename = str(uuid.uuid4())
        tmp=os.path.join(indat, filename)

        #with open(tmp, "a") as tmpfile:
        for yr in years_str:
            if data == 'chirps_monthly':
                pattern=mapset + "_" + yr + "_*"
            elif data == 'tamsat_monthly' or data == 'tamsat_daily':
                pattern="tamsat" + "_rfe" + yr + "_*"
            elif data == 'arc2_daily':
                pattern=pat + "_" + yr + "*"
            else:
                pattern=pat + "_" + yr + "_*" + p1
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
                v.rast_stats(map='bnd', raster=in2, column_prefix=in2, method=method)

        if sh or gr or cr or sg:
            r.mask(flags="r")

        now = datetime.now()
        a = int(now.strftime('%Y%m%d%H%M%S'))

        #outfile="stats_" + str(a) + "_" + suffixcsv + ".csv"
        outfile=outcsv + ".csv"
        outfile1=os.path.join(outfolder, outfile)
        v.out_ogr(input='bnd', output=outfile1, format='CSV', flags="m")
        user.close()
    finally:
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
