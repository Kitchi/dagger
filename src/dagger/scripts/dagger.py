#! /usr/bin/env python

import os
import shutil
import htcondor
from htcondor import dags

import argparse
import logging

logging.basicConfig(level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(message)s",
                handlers=[logging.FileHandler("write_dag.log"), logging.StreamHandler()])

def main():
    parser = argparse.ArgumentParser(description='Generate a DAG and submit it.')
    parser.add_argument('MS', type=str, help='MS file name')
    parser.add_argument('njob', type=int, help='Number of jobs to submit')
    parser.add_argument('SPWs', type=int, nargs='+', help='Spectral windows to process')
    parser.add_argument('DAG', type=str, help='DAG file name')
    parser.add_argument('--pre', '-p', type=str, required=False, help='PRE script file name')
    parser.add_argument('--post', '-P', type=str, required=False, help='POST script file name')
    parser.add_argument('--submit', '-S', action='store_true', help='Submit the DAG. If not passed, DAG will be written to disk but not submitted.')
    parser.add_argument('--verbose', '-v', action='store_true', help='Print out debugging and other info')
    parser.add_argument('--outdir', '-o', type=str, help='Output directory to store split MS and tar files')
    parser.add_argument('--clobber_ms', action='store_true', help='Overwrite output MS if it exists.')
    parser.add_argument('--clobber_tar', action='store_true', help='Overwrite output tarfile if it exists.')
    parser.add_argument('--cleanup', action='store_true', help='Wipe the input files at the end of execution.')
    parser.add_argument('--submit_args', type=str, help="Lines to put into the submit file formatted as key value pairs. If nothing is provided, will override the defaults")

    args = parser.parse_args()

    if args.pre is not None:
         write_pre(args)
    if args.post is not None:
        write_post(args)

    write_dag_submit_file(args)

