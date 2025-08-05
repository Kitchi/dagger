"""
Module for all the wrapper classes that will place the wrapped Python functions
into their own scripts, and within a DAG framework via HTCondor DAGMAN.
"""

import inspect
import os

import htcondor2
from htcondor2 import dags


class Dagger:
    """
    Store DAG state, provide methods to wrap Python functions, turn them into submit scripts,
    and add layers to the DAG, define parent/child relationships, and submit the DAG to HTCondor.
    """

    def __init__(self, dag_dir: str, dag_name: str, overwrite_dag_dir: bool = False):
        """
        Initialize the dagger object with a DAG name and directory.

        :param dag_dir: Directory where the DAG files, scripts, and submit files will be stored.
        :type dag_dir: str
        :param dag_name: Name of the DAG.
        :type dag_name: str
        """
        self.dag_dir = dag_dir
        self.dag_name = dag_name
        self.overwrite_dag_dir = overwrite_dag_dir

        self.dag = dags.DAG()
        self._nlayers = 1
        self._submit_functions = {}
        self._layer_list = []
        self._job_list = []
        self._job_names = []

        self._setup()

    def _setup(self):
        """
        This method creates the DAG directory if it does not exist and initializes
        the DAG object with the specified name.
        """
        if not os.path.exists(self.dag_dir):
            os.makedirs(self.dag_dir)

        if self.overwrite_dag_dir:
            # Clear existing files in the dag_dir if overwrite is enabled
            for file in os.listdir(self.dag_dir):
                file_path = os.path.join(self.dag_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)

    @property
    def submit_functions(self) -> dict:
        """
        Return the dictionary of functions that have been submitted to the DAG.
        This is useful for debugging and checking which functions are part of the DAG.

        :return: Dictionary of submitted functions.
        :rtype: dict
        """
        return self._submit_functions

    @property
    def layer_list(self) -> list:
        """
        Return the list of layers in the DAG. This is useful for debugging and checking
        the structure of the DAG.

        :return: List of layers in the DAG.
        :rtype: list
        """
        return self._layer_list

    @property
    def job_list(self) -> list:
        """
        Return the list of jobs in the DAG. This is useful for debugging and checking
        the jobs that have been added to the DAG.

        :return: List of jobs in the DAG.
        :rtype: list
        """
        return self._job_list

    @property
    def job_names(self) -> list:
        """
        Return the list of job names in the DAG. This is useful for debugging and checking
        the names of the jobs that have been added to the DAG.

        :return: List of job names in the DAG.
        :rtype: list
        """
        return self._job_names

    def parse_function(
        self,
        func: callable,
        return_as_string: bool = True,
        return_name: bool = False,
        trim_whitespace: bool = True,
    ) -> str | list[str]:
        """
        Parse a Python function and turn it into a list or string
        representation. This function uses the Python inspect module to
        retrieve the source code. This can be dangerous on unverified code, so
        please make sure you know what the source code is doing before wrapping
        it.

        The function can be return as a list of strings, or formatted into
        a valid string that can be directly placed into a Python script.
        Formatting as a list allows for further manipulation and modification
        of the code prior to writing it to a file, for example to add command
        line argument parsing, shebangs etc.

        :param func: The function to parse.
        :type func: callable
        :param return_as_string: If True, return the function as a formatted string.
                                 If False, return as a list of strings.
        :type return_as_string: bool
        :param return_name: If True, return the function name as well.
                            This is useful if you want to use the function name in the script.
        :type return_name: bool
        :param trim_whitespace: If True, trim leading whitespace from the function source code.
        This will not trim _all_ the whitespace, but only the global indentation level.
        :type trim_whitespace: bool

        :return: The function source code as a string or list of strings.
        :rtype: str or list
        :raises TypeError: If the input is not a callable function.

        :example:
        >>> def example_function(x: int, y: str) -> None:
        ...     print(f"Example function with x={x} and y={y}")
        >>> dagger_instance = dagger()
        >>> dagger_instance.parse_function(example_function)
        """

        if not callable(func):
            raise TypeError("Input must be a callable function.")

        name = func.__name__
        funcstr = inspect.getsource(func)
        funcstr = funcstr.split("\n")[1:]

        # signature = inspect.signature(func)
        # sigstr = f"{name}{signature}:"
        # funcstr = [sigstr] + funcstr

        if trim_whitespace:
            # Trim leading whitespace from the function source code
            # This will not trim _all_ the whitespace, but only the global indentation level
            # Assume first line is never indented, and use that to determine the indent level
            indent_level = len(funcstr[0]) - len(funcstr[0].lstrip())
            funcstr = [line[indent_level:] for line in funcstr]

        if return_as_string:
            funcstr = "\n".join(funcstr)

        if return_name:
            return funcstr, name
        else:
            return funcstr

    def func_to_submit_obj(
        self,
        func: callable,
        py_script_name: str = "",
        submit_args: dict = {},
    ) -> htcondor2.Submit:
        """
        Convert a Python function into a HTCondor Submit object.

        :param func: The function to convert.
        :type func: callable
        :param py_script_name: The name of the Python script to create.
                            If not provided, it defaults to the function name with a `.py` extension.
                            The Python script is created when this function is called. The name can
                            contain the full path to the script. If only a name is provided, it will be generated
                            in the current directory.
        :type py_script_name: str
        :param submit_script_name: The name of the submit script to create.
                            If not provided, it defaults to the function name with a `.submit` extension.
        :type submit_script_name: str
        :param submit_args: Additional arguments to include in the submit script.
                            This can include command line arguments, Condor requirements, container paths etc.

        :return: HTCondor Submit object
        :rtype: htcondor2.dags.Submit

        :raises TypeError: If the input is not a callable function.
        """

        if not callable(func):
            raise TypeError("Input must be a callable function.")

        funcstr, funcname = self.parse_function(
            func, return_as_string=True, return_name=True
        )

        if len(py_script_name) == 0:
            py_script_name = os.path.join(self.dag_dir, f"{func.__name__}.py")
        elif not os.path.isabs(py_script_name):
            # If the script name is not absolute, make it relative to the dag_dir
            py_script_name = os.path.join(self.dag_dir, py_script_name)

        with open(py_script_name, "w") as f:
            f.write(funcstr)

        # This has to be a relative path to the script, not an absolute path
        # because HTCondor will look for the script in the current working directory
        submit_dict = {
            "executable": os.path.basename(py_script_name),
        }
        submit_dict.update(submit_args)

        submit_obj = htcondor2.Submit(submit_dict)

        self._submit_functions[funcname] = submit_obj

        return submit_obj

    def dag_layer(
        self,
        submit_obj: htcondor2.Submit,
        submit_vars: list[dict],
        layer_name: str = "",
        parent_layer_name: str = "",
        **kwargs,
    ) -> dags.NodeLayer:
        """
        Create a new layer in the DAG. If no layer name is provided,
        a default name will be generated based on the current number of layers,
        which is incremented each time a new layer is created.

        :param submit_obj: The HTCondor Submit object to associate with this layer.
        :type submit_obj: htcondor2.Submit
        :param submit_vars: Additional arguments to include in the submit script for this layer.
                            This can include command line arguments, Condor requirements, container paths etc.
                            Each dictionary in the list represents a set of variables to be used
                            for a single job in the layer.
        :type submit_vars: list[dict]

        :param layer_name: Optional : Name of the layer to create. If not provided,
                           a default name will be generated based on the current number of layers.
                           The default format is "layer_{n}", where n is the current number of layers.
        :type layer_name: str

        :param parent_layer_name: Name of the parent layer to link to.
                                  If provided, this layer will be added as a child of the parent layer.
        :type parent_layer_name: str

        :param kwargs: Additional keyword arguments to pass to the layer creation.
                       This can include any additional parameters supported by htcondor2.dags.NodeLayer.
        :type kwargs: dict

        :raises ValueError: If parent_layer_name does not exist in the DAG.
        :raises TypeError: If submit_obj is not an instance of htcondor2.Submit.

        :return: A NodeLayer object representing the new layer in the DAG.
        :rtype: htcondor2.dags.NodeLayer

        :example:
        >>> dag = Dagger()
        >>> dag.dag_layer(layer_name="my_layer")
        >>> dag.dag_layer(parent_layer_name="my_layer")
        """

        if not isinstance(submit_obj, htcondor2.Submit):
            raise TypeError(
                "submit_obj must be an instance of htcondor2.Submit or None."
            )

        # Check if parent layer isspecified and exists in the DAG
        if parent_layer_name and parent_layer_name not in self._layer_list:
            raise ValueError(
                f"Parent layer '{parent_layer_name}' does not exist in the DAG. "
                "Please define it first before adding any children."
            )

        if not layer_name:
            layer_name = f"layer_{self._nlayers}"

        if parent_layer_name:
            idx = self._layer_list.index(parent_layer_name)
            job = self.job_list[idx].child_layer(
                name=layer_name,
                submit_description=submit_obj,
                vars=submit_vars,
                **kwargs,
            )
        else:
            job = self.dag.layer(
                name=layer_name,
                submit_description=submit_obj,
                vars=submit_vars,
                **kwargs,
            )

        self._nlayers += 1
        self._layer_list.append(layer_name)
        self._job_list.append(job)
        self._job_names.append(job.name)

        return job

    def func_to_layer(
        self,
        func: callable,
        py_script_name: str = "",
        submit_args: dict = {},
        layer_name: str = "",
        parent_layer_name: str = "",
        layer_vars: list[dict] = [],
        **kwargs,
    ) -> dags.NodeLayer:
        """
        Convert a Python function into a layer in the DAG. This is a convenience method that combines
        the `func_to_submit_obj` and `dag_layer` methods into one. It will automatically convert the function
        to a submit object and add it to the DAG as a layer.

        :param func: The function to convert.
        :type func: callable
        :param py_script_name: The name of the Python script to create.
                               If not provided, it defaults to the function name with a `.py` extension.
        :type py_script_name: str
        :param submit_args: Additional arguments to include in the submit script.
                            This can include command line arguments, Condor requirements, container paths etc.
        :type submit_args: dict
        :param layer_name: Optional : Name of the layer to create. If not provided,
                           a default name will be generated based on the current number of layers.
                           The default format is "layer_{n}", where n is the current number of layers.
        :type layer_name: str
        :param parent_layer_name: Name of the parent layer to link to.
                                  If provided, this layer will be added as a child of the parent layer.
        :type parent_layer_name: str
        :param layer_vars: List of dictionaries representing variables to be used for each job in the layer.
                           Each dictionary in the list represents a set of variables to be used
                           for a single job in the layer.
        :type layer_vars: list[dict]
        :param kwargs: Additional keyword arguments to pass to the layer creation.
                       This can include any additional parameters supported by htcondor2.dags.NodeLayer.
        :type kwargs: dict
        :raises TypeError: If the input is not a callable function.

        :return: A NodeLayer object representing the new layer in the DAG.
        :rtype: htcondor2.dags.NodeLayer
        :example:
        >>> from dagger import Dagger
        >>> dag = Dagger(dag_dir="my_dag_dir", dag_name="my_dag")
        >>> def my_function(x: int, y: str) -> None:
        ...     print(f"My function with x={x} and y={y}")
        >>> layer = dag.func_to_layer(
        ...     func=my_function,
        ...     py_script_name="my_function.py",
        ...     submit_args={"requirements": "Machine == 'my_machine'"},
        ...     layer_name="my_layer",
        ...     parent_layer_name="parent_layer",
        ...     layer_vars=[{"x": 1, "y": "test"}, {"x": 2, "y": "example"}]
        ... )
        """

        if not callable(func):
            raise TypeError("Input must be a callable function.")

        submit_obj = self.func_to_submit_obj(
            func=func,
            py_script_name=py_script_name,
            submit_args=submit_args,
        )

        return self.dag_layer(
            submit_obj=submit_obj,
            submit_vars=layer_vars,
            layer_name=layer_name,
            parent_layer_name=parent_layer_name,
            **kwargs,
        )

    def add_func_to_layer(self, layer_name: str = "", **kwargs) -> callable:
        """

        A decorator to add a function to the DAG as a new layer.
        This is a convenience method that allows you to add a function to the DAG
        without having to manually create a submit object and layer. It will automatically
        convert the function to a submit object and add it to the DAG as a layer.

        The function will be converted to a HTCondor Submit object, and then added to the DAG as a layer.

        :param layer_name: Optional : Name of the layer to create. If not provided,
                           a default name will be generated based on the current number of layers.
                           The default format is "layer_{n}", where n is the current number of layers.
        :type layer_name: str
        :param kwargs: Additional keyword arguments to pass to the layer creation.
                       This can include any additional parameters supported by Dagger.dag_layer,
                       such as `submit_vars`, `parent_layer_name`, etc. as well as any additional
                       parameters supported by htcondor2.dags.NodeLayer.
        :type kwargs: dict

        :return: A NodeLayer object representing the new layer in the DAG.
        :rtype: htcondor2.dags.NodeLayer
        :raises TypeError: If the input is not a callable function.
        :example:
        >>> from dagger import Dagger
        >>> dag = Dagger(dag_dir="my_dag_dir", dag_name="my_dag")
        >>> @dag.add_func_to_layer(layer_name="my_layer", vars=[{"x": 1, "y": "test"}])
        >>> def my_function(x: int, y: str) -> None:
        ...     print(f"My function with x={x} and y={y}")
        >>> my_function(1, "test")  # This will add the function to the DAG
        """

        def decorator(func: callable) -> callable:
            if not callable(func):
                raise TypeError("Input must be a callable function.")

            def wrapper(*args, **kwargs):
                submit_obj = self.func_to_submit_obj(func, **kwargs)
                return self.dag_layer(
                    submit_obj=submit_obj,
                    submit_vars=kwargs.get("vars", [{}]),
                    layer_name=layer_name,
                    **kwargs,
                )

            return wrapper

        return decorator

    def write_dag(self, **kwargs) -> None:
        """
        Write the current DAG to a file. This will create a .dag file that can be submitted to HTCondor.
        This is a simple wrapepr around the htcondor2.dags.write_dag method, for ease of use with
        this class.

        :param kwargs: Keyword arguments to pass to the DAG writing function.
                       This can include any parameters supported by htcondor2.dags.write_dag
        :type kwargs: dict
        :example:
        >>> dag = Dagger()
        >>> dag.write_dag(filename="my_dag.dag", submit_description="my_submit.sub")

        :return: None
        """

        dags.write_dag(
            self.dag,
            dag_dir=self.dag_dir,
            dag_file_name=f"{self.dag_name}.dag",
            **kwargs,
        )


# TODO : Add the ability to read common attributes from a TOML file or something
# TODO : Add a @dagger.script(name='name') decorator to automatically create a script object that can be referenced by name
