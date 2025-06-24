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



class DAGWriterBase:
    def __init__(self):
        pass


    def write_pre(self, inp_str, shebang = '#! /bin/bash', mode = 'w', script_name=''):
        """
        A simple wrapper to dump the input string into a bash script,
        fix the execute permissions.

        The PRE script is typically not run within a container/virtual environment etc.

        Inputs:

        inp_str,str         Correctly formatted string that will get placed
                            into the PRE script
        shebang             The shebang to put at the top of the script, default : #! /bin/bash
        mode                Mode to open the file, default 'w'
        script_name, str    Name of the PRE script file, if not specified is placed into `PRE.script`

        Returns:
        pre_script_name     Name of the script
        """

        script_str = ""
        script_str += shebang
        script_str += "\n"
        script_str += inp_str
        script_str += f"\n"

        if script_name == '':
            script_name = "PRE.script"

        with open(script_name, mode):
            fptr.write(script_str)

        os.chmod(script_name, '0755')

        #script_str += "~/soft/micromamba/envs/py312/bin/python /home/srikrishna.sekhar/src/dagger/src/dagger/scripts/split_ms.py " # No newline
        #script_str += f"{self.args.MS} {self.args.njob} {" ".join([str(spw) for spw in self.args.SPWs])} " # No newline
        #script_str += "--outdir {self.args.outdir} --clobber_ms {self.args.clobber_ms} --clobber_tar {self.args.clobber_tar}\n"

    def write_post(self):
        """
        Given the input command line arguments, pass them into the post-script
        generator program.

        Inputs:
        None

        Returns:
        None
        """

        print("Post script generation not yet supported, sorry!")


    def write_job_submit_file(self, inp_str, script_name='', job_id = None, file_mode='w'):
        """
        Simple wrapper to put the input string into a job submit script. This can be used
        as a stand-alone submit script, or passed in to a DAG node.

        Inputs:
        inp_str, str            Correctly formatted input string
        script_name, str        Name of the script, if omitted will save as "job_jobid.script"
        job_id, int             Job ID index, useful for multi-node DAGs
        file_mode, str          The file mode to use to open the output file, default='w'

        Returns:
        script_name, str 
        """

        submit_str = ""
        submit_str += inp_str
        submit_str = "\n"

        if script_name == '':
            script_name = f'job_{job_id}.script' if job_id is not None else 'job.script'

        with open(script_name, file_mode) as fptr:
            fptr.write(submit_str)
            fptr.write("\n")

    def write_cube_imaging_submit_file(self, args, use_default=True):
        """
        Write the primary submit file for the DAG.

        Inputs:
        args    Input command line arguments
        """

        submit_str = ""

        submit_args_default = {
            'gridder': 'mosaic',
            'imsize': '8192',
            'cell': '0.004arcsec',
            'stokes': 'I',
            'niter': '100000',
            'usemask': 'auto-multithresh',
            'threshold': '2mJy',
            '+SingularityImage' = "osdf:///path-facility/data/srikrishna.sekhar/containers/casa-6.6.0-modular.sif",
            '+WantOSPool'	= true,
            'executable' = tclean.py,
            '# Pass in command line args - params from tclean_params.htc
            'arguments' = "$(input_data) --jobid $(Process) --gridder $(gridder) --imsize $(imsize) --cell $(cell) --stokes $(stokes) --niter $(niter) --usemask $(usemask) --threshold $(threshold)",

            'transfer_input_files' = $(input_data),
            'should_transfer_files' = YES,
            'when_to_transfer_output'	= ON_EXIT_OR_EVICT,
            'request_cpus' = 1,
            'request_memory' = 50G,
            'request_disk' = 100G,
            'max_retries'	= 2,
            'log'	= tclean_$(Process).log,
            'output'	= tclean_$(Process).out,
            'error'	= tclean_$(Process).err,
            'unordered_lines' = '',
        }

        # If command line arguments are passed, prefer those over defaults
        if args.submit_args is not None:
            import ast
            submit_args_cmd = ast.literal_eval(args.tclean_args)
            if use_default is True:
                submit_args = submit_args_default | submit_args_cmd
            else:
                submit_args = submit_args_cmd
        elif use_default is True:
            submit_args = submit_args_default
        else:
            logging.error(f"No user specified dict, and user does not want to use defaults. Cannot proceed with DAG.")
            raise ValueError()

        for key, val in submit_args.items():
            if key == 'unordered_lines':
                submit_str += f"{str(key)}"
            else:
                submit_str += f"{str(key)} = {str(val)}\n"

        with open(args.DAG, 'w') as fptr:
            fptr.write(submit_str)



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

