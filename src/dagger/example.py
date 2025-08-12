from dagger import Dagger


def test_func(a: int, b: float) -> str:
    """
    A simple test function to demonstrate the parsing functionality.
    """
    print("This is a test function.")
    return "Test complete."


if __name__ == "__main__":
    dag = Dagger(dag_dir="test", dag_name="test_dag")
    submit_obj = dag.function_to_submit_obj(test_func)
    print(dag.submit_functions["test_func"])
    job = dag.dag_layer(
        submit_obj=submit_obj,
        submit_vars=[
            {"arg1": "value1", "arg2": "value2"},
        ],
        layer_name="test_layer",
    )
    print(f"Created layer: {job}")
    print(dag.layer_list)
    job2 = dag.dag_layer(
        submit_obj=submit_obj,
        submit_vars=[
            {"arg1": "value3", "arg2": "value4"},
        ],
        layer_name="test_layer2",
        parent_layer_name="test_layer",
    )
    print(f"Created layer with parent: {job2}")

    dag.write_dag()
