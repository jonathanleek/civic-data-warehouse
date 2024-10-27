import json


def create_dependency_graph(file_path):
    # Load the serialized DAG from a JSON file
    with open(file_path, 'r') as f:
        serialized_dag = json.load(f)

    # Create a dictionary to store the graph representation
    graph = {}

    # Extract tasks from the serialized DAG
    tasks = serialized_dag['dag']['tasks']

    # Initialize the graph with task IDs
    for task in tasks:
        graph[task['task_id']] = task.get('downstream_task_ids', [])

    # Helper function to format the graph
    def format_dependency(task_id, downstream_tasks):
        return " >> ".join([task_id] + downstream_tasks)

    # Create the dependency graph representation
    dependency_graph = []
    for task_id, downstream_tasks in graph.items():
        if downstream_tasks:
            dependency_graph.append(format_dependency(task_id, downstream_tasks))

    return dependency_graph


# Function to convert 'null' to None in the dictionary
def convert_nulls(data):
    if isinstance(data, dict):
        return {k: convert_nulls(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_nulls(i) for i in data]
    elif data is None:
        return None
    elif data == 'null':
        return None
    else:
        return data


# Assuming the serialized DAG is stored in 'serialized_dag.json'
file_path = 'serialized_dag.json'

# Generate and print the dependency graph
print(create_dependency_graph(file_path))
