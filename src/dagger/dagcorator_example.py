from dagger import Dagcorator


def test_func_1(input_data: str) -> str:
    """
    First step in the workflow - processes input data.
    """
    print(f"Processing input: {input_data}")
    return f"processed_{input_data}"


def test_func_2(processed_data: str) -> str:
    """
    Second step in the workflow - analyzes processed data.
    """
    print(f"Analyzing: {processed_data}")
    return f"analyzed_{processed_data}"


def test_func_3(analyzed_data: str) -> str:
    """
    Final step in the workflow - generates report.
    """
    print(f"Generating report from: {analyzed_data}")
    return f"report_{analyzed_data}"


if __name__ == "__main__":
    # Create a decorator-based DAG builder
    dagcorator = Dagcorator(dag_dir="decorator_test", dag_name="workflow_dag")
    
    # Decorate functions to add them to the DAG
    @dagcorator.layer(layer_name="process_step", layer_vars=[{"input_data": "data1.txt"}, {"input_data": "data2.txt"}])
    def process_data(input_data: str) -> str:
        return test_func_1(input_data)
    
    @dagcorator.layer(layer_name="analyze_step", parent_layer_name="process_step", layer_vars=[{"processed_data": "proc1"}, {"processed_data": "proc2"}])
    def analyze_data(processed_data: str) -> str:
        return test_func_2(processed_data)
    
    @dagcorator.layer(layer_name="report_step", parent_layer_name="analyze_step", layer_vars=[{"analyzed_data": "analysis1"}])
    def generate_report(analyzed_data: str) -> str:
        return test_func_3(analyzed_data)
    
    # Print DAG information
    print(f"Created DAG with {len(dagcorator.layer_list)} layers:")
    for layer_name in dagcorator.layer_list:
        print(f"  - {layer_name}")
    
    print(f"Submit functions: {list(dagcorator.submit_functions.keys())}")
    
    # Write the DAG to file
    dagcorator.write_dag()
    print(f"DAG written to {dagcorator.dag_dir}/workflow_dag.dag")
    
    # Functions can still be called normally
    result1 = process_data("test_input")
    print(f"Direct function call result: {result1}")