import os
import yaml
import requests

# Set up API Authentication (OAuth2)
PREFECT_API_URL = os.getenv("PREFECT_API_URL")  # Set this to your Prefect API URL
OAUTH_TOKEN = os.getenv("OAUTH_TOKEN")          # OAuth2 token for API access
HEADERS = {
    "Authorization": f"Bearer {OAUTH_TOKEN}",
    "Content-Type": "application/json",
}

def load_deployment_file(filepath: str) -> dict:
    """Load deployments.yaml from the repository."""
    with open(filepath, "r") as file:
        deployments = yaml.safe_load(file)
    return deployments

def get_or_create_flow_id(flow_name: str) -> str:
    """Get the flow_id using the Prefect REST API with the flow_name, create it if it doesn't exist."""
    # First try to get the flow_id by name
    flow_url = f"{PREFECT_API_URL}/flows/name/{flow_name}"
    response = requests.get(flow_url, headers=HEADERS)
    
    if response.status_code == 200:
        return response.json().get('id')
    
    elif response.status_code == 404:
        # Flow not found, so create it
        print(f"Flow {flow_name} not found. Creating new flow...")
        create_flow_url = f"{PREFECT_API_URL}/flows"
        flow_data = {
            "name": flow_name
        }
        create_response = requests.post(create_flow_url, headers=HEADERS, json=flow_data)
        if create_response.status_code == 201:
            print(f"Flow {flow_name} created successfully.")
            return create_response.json().get('id')
        else:
            print(f"Failed to create flow {flow_name}: {create_response.text}")
            return None
    else:
        print(f"Failed to fetch flow_id for flow_name {flow_name}: {response.text}")
        return None

def fetch_deployment(flow_name: str, deployment_name: str) -> dict:
    """Fetch the deployment details from the server by its flow_name and deployment_name."""
    deployment_url = f"{PREFECT_API_URL}/deployments/name/{flow_name}/{deployment_name}"
    response = requests.get(deployment_url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    return None

def detect_type(value):
    """Detect the type of a given parameter value for the parameter_openapi_schema."""
    if isinstance(value, int):
        return "integer"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, float):
        return "number"
    elif isinstance(value, list):
        return "array"
    else:
        return "string"  # Default to string for other types

def generate_default_openapi_schema(parameters: dict) -> dict:
    """Generate a default OpenAPI schema based on provided parameters."""
    schema = {
        "type": "object",
        "properties": {}
    }
    for param_name, param_value in parameters.items():
        schema["properties"][param_name] = {"type": detect_type(param_value)}
    return schema

def compare_deployments(deployment_data: dict, server_deployment: dict) -> bool:
    """Compare the deployment data with the server version and return True if they differ."""
    fields_to_compare = [
        'is_schedule_active', 'paused', 'schedules', 'description', 'tags', 'parameters', 
        'pull_steps', 'work_queue_name', 'work_pool_name', 'storage_document_id', 
        'infrastructure_document_id', 'path', 'version', 'entrypoint', 'job_variables',
        'enforce_parameter_schema', 'parameter_openapi_schema'
    ]
    
    for field in fields_to_compare:
        if deployment_data.get(field) != server_deployment.get(field):
            return True  # Differences found, an update is needed
    return False  # No differences found, no update needed

def create_or_update_deployment(deployment_data: dict):
    """Create or update a deployment using the Prefect REST API."""
    deployment_name = deployment_data.get('name')
    flow_name = deployment_data.get('flow_name')  # Use the flow_name from the YAML file

    # Get or create the flow_id using the provided flow_name
    flow_id = get_or_create_flow_id(flow_name)
    
    if flow_id:
        # Fetch the existing deployment from the server, if it exists
        server_deployment = fetch_deployment(flow_name, deployment_name)

        # If parameters exist and no parameter_openapi_schema is provided, generate one
        if deployment_data.get('parameters') and not deployment_data.get('parameter_openapi_schema'):
            print(f"Generating default parameter_openapi_schema for deployment {deployment_name}")
            deployment_data['parameter_openapi_schema'] = generate_default_openapi_schema(deployment_data['parameters'])

        # Prepare deployment data according to the schema
        deployment_data_filtered = {
            "name": deployment_data.get('name'),
            "flow_id": flow_id,
            "is_schedule_active": deployment_data.get('is_schedule_active', True),
            "paused": deployment_data.get('paused', False),
            "schedules": deployment_data.get('schedules', []),
            "description": deployment_data.get('description', ""),
            "tags": deployment_data.get('tags', []),
            "parameters": deployment_data.get('parameters', {}),
            "pull_steps": deployment_data.get('pull_steps', []),
            "work_queue_name": deployment_data.get('work_pool', {}).get('work_queue_name'),
            "work_pool_name": deployment_data.get('work_pool', {}).get('name'),
            "storage_document_id": deployment_data.get('storage_document_id', None),
            "infrastructure_document_id": deployment_data.get('infrastructure_document_id', None),
            "schedule": deployment_data.get('schedule', None),  # If needed, you can also handle the schedule
            "path": deployment_data.get('path', ""),
            "version": deployment_data.get('version', ""),
            "entrypoint": deployment_data.get('entrypoint', ""),
            "job_variables": deployment_data.get('job_variables', {}),
            "enforce_parameter_schema": deployment_data.get('enforce_parameter_schema', True),
            "parameter_openapi_schema": deployment_data.get('parameter_openapi_schema', {}),
        }

        # Compare deployments if server version exists
        if server_deployment:
            if compare_deployments(deployment_data_filtered, server_deployment):
                # Differences detected, update the deployment
                deployment_url = f"{PREFECT_API_URL}/deployments"
                response = requests.post(deployment_url, headers=HEADERS, json=deployment_data_filtered)
                if response.status_code in [200, 201]:
                    print(f"Deployment {deployment_name} updated successfully.")
                else:
                    print(f"Failed to update deployment {deployment_name}: {response.text}")
            else:
                # No differences detected, skip update
                print(f"Deployment {deployment_name} is already up-to-date. No update necessary.")
        else:
            # Deployment doesn't exist, create it
            deployment_url = f"{PREFECT_API_URL}/deployments"
            response = requests.post(deployment_url, headers=HEADERS, json=deployment_data_filtered)
            if response.status_code in [200, 201]:
                print(f"Deployment {deployment_name} created successfully.")
            else:
                print(f"Failed to create deployment {deployment_name}: {response.text}")
    else:
        print(f"Failed to find or create flow_id for flow_name {flow_name}, skipping deployment creation.")

def delete_deployment(deployment_id: str):
    """Delete a deployment using the Prefect REST API."""
    delete_url = f"{PREFECT_API_URL}/deployments/{deployment_id}"
    response = requests.delete(delete_url, headers=HEADERS)
    if response.status_code == 204:
        print(f"Successfully deleted deployment with ID: {deployment_id}")
    else:
        print(f"Failed to delete deployment: {response.text}")

def delete_flow(flow_id: str):
    """Delete a flow using the Prefect REST API."""
    delete_url = f"{PREFECT_API_URL}/flows/{flow_id}"
    response = requests.delete(delete_url, headers=HEADERS)
    if response.status_code == 204:
        print(f"Successfully deleted flow with ID: {flow_id}")
    else:
        print(f"Failed to delete flow: {response.text}")

def list_deployments() -> list:
    """List all deployments using the Prefect REST API."""
    list_url = f"{PREFECT_API_URL}/deployments/filter"
    response = requests.post(list_url, headers=HEADERS, json={})
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch deployments: {response.text}")
        return []

def list_flows() -> list:
    """List all flows using the Prefect REST API."""
    list_url = f"{PREFECT_API_URL}/flows/filter"
    response = requests.post(list_url, headers=HEADERS, json={})
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch flows: {response.text}")
        return []

def compare_and_delete_old_deployments(current_deployments: list, yaml_deployments: list):
    """Compare the deployments on the server and YAML file, and delete any missing from YAML."""
    yaml_deployment_names = {deployment['name'] for deployment in yaml_deployments}

    # Find deployments on the server that are not in the YAML file
    for deployment in current_deployments:
        if deployment['name'] not in yaml_deployment_names:
            print(f"Deployment {deployment['name']} is missing from YAML and will be deleted.")
            delete_deployment(deployment['id'])

def compare_and_delete_old_flows(current_flows: list, yaml_flows: list):
    """Compare the flows on the server and YAML file, and delete any missing from YAML."""
    yaml_flow_names = {deployment['flow_name'] for deployment in yaml_flows}

    # Find flows on the server that are not in the YAML file
    for flow in current_flows:
        if flow['name'] not in yaml_flow_names:
            print(f"Flow {flow['name']} is missing from YAML and will be deleted.")
            delete_flow(flow['id'])

def deploy_from_yaml(filepath: str):
    """Deploy or update all flows and deployments from a YAML file, and delete removed deployments."""
    # Load deployments from YAML
    yaml_deployments = load_deployment_file(filepath).get('deployments', [])

    # Deploy or update each deployment from the YAML file
    for deployment in yaml_deployments:
        create_or_update_deployment(deployment)

    # Fetch existing deployments and flows from the Prefect server
    current_deployments = list_deployments()
    current_flows = list_flows()

    # Delete old deployments and flows that are no longer in the YAML file
    compare_and_delete_old_deployments(current_deployments, yaml_deployments)
    compare_and_delete_old_flows(current_flows, yaml_deployments)

if __name__ == "__main__":
    # Example usage
    deployments_file = "deployments.yml"
    
    # Deploy all flows and deployments from YAML and remove old ones
    deploy_from_yaml(deployments_file)
