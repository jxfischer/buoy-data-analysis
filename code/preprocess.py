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
    for buoy in args.buoy:
        datadir=buoydir(buoy)
        if not isdir(datadir):
            parser.error("Did not find data directory %s"%datadir)
    print("Processing data for buoy %d" %args.buoy)
    return 0


if __name__=="__main__":
    sys.exit(main())
    
