#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 15:17:31 2019

@author: jason
"""
import pandas as pd
import os
from os.path import abspath, join, expanduser, isdir, dirname
import argparse
import sys

from dataworkspaces.lineage import LineageBuilder

from utils import read_file

BASEDIR=abspath(expanduser(join(dirname(__file__), '..')))
OUTDIR=join(BASEDIR,"intermediate-data")

def buoydir(buoy):
    return join(BASEDIR, "data/%s"%buoy)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("buoy",metavar="BUOY", type=int, nargs="+",
                        help="buoy number")
    args=parser.parse_args()
    if not isdir(OUTDIR):
        parser.error("missing output directory %s"%OUTDIR)
    input_dirs = []
    for buoy in args.buoy:
        datadir=buoydir(buoy)
        if not isdir(datadir):
            parser.error("Did not find data directory %s"%datadir)
        input_dirs.append(datadir)

    with LineageBuilder().as_script_step().with_parameters({}).with_input_paths(input_dirs).eval() as lineage:
        lineage.add_output_path(OUTDIR)
        for buoy in args.buoy:
            print("Processing data for buoy %d" %buoy)
            years= []
            datadir=buoydir(buoy)
            for fname in sorted(os.listdir(datadir)):
                fpath=join(datadir, fname)
                years.append(read_file(fpath))
            yeardf=pd.concat(years, sort=False)
            yeardf.to_csv(join(OUTDIR,"processed_%d.csv.gz"%buoy), compression="gzip")
            print ("buoy %d has %d columns and %d rows"%(buoy, len(yeardf.columns), len(yeardf)))
    return 0


if __name__=="__main__":
    sys.exit(main())
    
