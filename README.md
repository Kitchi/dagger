<div align="center">
  
  # Dagger
  Python wrapper around the DAGMan [HTCondor Python bindings](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/index.html) for
  quality-of-life improvements while generating DAGs
</div>

 <img src="https://i.imgur.com/NAGahA2.png" align='left' width='15%'>

`Dagger` is a fairly thin Python wrapper around the HTCondor Python bindings that simplifies the process of creating Directed Acyclic Graphs (DAGs) for HTCondor jobs. It
provides a higher-level interface to define job scripts, Submit scripts, and DAG layers and dependencies. 
It provides the functionality to go from a fully standalone Python function directly into a DAG, without the need to
manually manage any of the intermediate steps. A typical DAGMan workflow involves 

      1. Writing the job script (e.g., a Python script)
      2. Writing an HTCondor submit file that defines the job (e.g., the executable, arguments, 
         environment variables, etc.)
      3. Writing a DAG file that registers the job to a DAGMan Layer
      4. Repeating (1) through (3) for every DAG layer
    
`Dagger` automates this process by allowing you to define your job script, submit file, and DAG layer in a single Python script,
making for easier management and less boilerplate code.

> [!WARNING]
> This is seriously **alpha** software, please use at your own risk. 

## Example Usage

```
from dagger import Dagger

# This function will be turned into a standalone script, so it needs to be self-sufficient.
# Command line parsing can happen within the function, but any arguments passed in to the
# function will _not_ be respected
def my_function():
    import numpy as np
    import argparse

    parse = argparse.ArgumentParser()
    parser.add_argument('my_input', type=int)
    args = parser.parse_args()

    out = np.sqrt(parser.my_input)
    return out


if __name__ == __main__':
    dg = Dagger(dag_dir='./path/to/dag_dir', dag_name='amazing_dag')

    # Turn the Python function into a HTCondor Submit file
    sub_obj = dg.func_to_submit_file(my_function)

    # Submit vars contains _all_ the other information
    # that the submit file needs to successfully run on
    # HTCondor
    submit_vars = []
    for i in range(10): # Assume 10 jobs in this DAG layer
        submit_vars.append({
          'my_input' : i,
          'executable' : 'my_function.py',
          'arguments' : '$(my_input)',
          'request_cpus' : 1,
          'request_memory' : '1G',
          'request_disk' : '1G'
        })

    # Add a layer to the DAG
    dg.dag_layer(sub_obj, layer_name = 'A', submit_vars=submit_vars)

    # Write the DAG and associated files to disk - this will generate
    # the Python script, submit file, DAGMAN file etc.
    dg.write_dag()
```
