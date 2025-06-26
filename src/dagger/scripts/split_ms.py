#! /usr/bin/env python

import os
import shutil
import htcondor
from htcondor import dags

from casatools import msmetadata
from casatasks import split
msmd = msmetadata()

import argparse
import tarfile
import logging

from dagger import mshandler

logging.basicConfig(level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(message)s",
                handlers=[logging.FileHandler("write_dag.log"), logging.StreamHandler()])

def main():
    parser = argparse.ArgumentParser(description='Generate a DAG and submit it.')
    parser.add_argument('MS', type=str, help='MS file name')
    parser.add_argument('njob', type=int, help='Number of jobs to submit')
    parser.add_argument('SPWs', type=int, nargs='+', help='Spectral windows to process')
    parser.add_argument('--outdir', '-o', type=str, help='Output directory to store split MS and tar files')
    parser.add_argument('--clobber_ms', action='store_true', help='Overwrite output MS if it exists.')
    parser.add_argument('--clobber_tar', action='store_true', help='Overwrite output tarfile if it exists.')

    args = parser.parse_args()

    mshandler = MSHandler(args.MS, args.SPWs, args.verbose)
    
    # nspw:int, nchan:list
    nspw, nchan = mshandler.get_ms_info()
    total_chans = sum(nchan)
    
    # Get a list of SPW selections depending on the number of jobs
    # requested
    spw_selections = mshandler.get_spw_selections(nchan, args.njob)
    if args.verbose:
        logging.info(f"SPW selections are {spw_selections}")
    
    tarnames = mshandler.split_ms(spw_selections, output_dir=args.outdir, clobber_ms=args.clobber_ms, clobber_tar=args.clobber_tar)

    with open('output_file_names.txt', 'w') as ffile:
        pass

