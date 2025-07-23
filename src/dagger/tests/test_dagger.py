import htcondor2

from dagger.dagger import Dagger


def test_parse_function():
    """
    Test the parse_function method of the dagger class.
    """
    dag = Dagger()

    def sample_function(x: int, y: str) -> str:
        """
        A sample function to test parsing.
        """
        return f"Received {x} and {y}"

    # Parse the function and get the string representation
    func_str = dag.parse_function(sample_function, return_as_string=True)

    # Check if the string representation contains the function name and parameters
    assert "def sample_function(x: int, y: str) -> str:" in func_str
    assert 'return f"Received {x} and {y}"' in func_str


def test_parse_function_as_list():
    """
    Test the parse_function method of the dagger class with return_as_string=False.
    """
    dag = Dagger()

    def another_function(a: float, b: float) -> float:
        """
        Another sample function to test parsing.
        """
        return a + b

    # Parse the function and get the list representation
    func_list = dag.parse_function(another_function, return_as_string=False)

    # Check if the list contains the function signature and body
    assert "def another_function(a: float, b: float) -> float:" in func_list[0]
    assert "return a + b" in func_list[-1]


def test_parse_function_invalid_input():
    """
    Test the parse_function method of the dagger class with invalid input.
    """
    dag = Dagger()

    # Check if TypeError is raised when input is not callable
    try:
        dag.parse_function("not_a_function")
    except TypeError as e:
        assert str(e) == "Input must be a callable function."
    else:
        assert False, "Expected TypeError was not raised."


def test_dag_initialization():
    """
    Test the initialization of the dagger class.
    """
    dag = dagger(dag_name="dagger")

    # Check if the DAG name is set correctly
    assert dag.dag_name == "dagger"

    # Check if the DAG object is initialized
    assert isinstance(dag.dag, dag.dags.DAG)

    # Check if the number of layers is initialized to 1
    assert dag._nlayers == 1


def test_submit_from_function():
    """
    Test the submit_from_function method of the dagger class.
    """
    dag = Dagger()

    def test_function(x: int, y: str) -> str:
        """
        A test function to submit to the DAG.
        """
        return f"Test function with x={x} and y={y}"

    # Submit the function to the DAG
    submit_obj = dag.submit_obj_from_function(test_function)
    submit_str = submit_obj.__repr__()

    # Check if the function is added to the DAG
    assert type(submit_obj) == htcondor2.Submit
    assert f"executable = test_function.py" in submit_str
