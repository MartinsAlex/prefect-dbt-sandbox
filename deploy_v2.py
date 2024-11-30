import requests
import yaml
import json
import logging
import sys


#TODO: add default pull step based on instance + branch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger()

DEBUG_MODE = False

if DEBUG_MODE:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


API_BASE_URL = "http://127.0.0.1:4200/api"
API_SIMPLE_AUTH_USER = "prefect-analytics"
API_SIMPLE_AUTH_PASSWORD = "1234"
HEADERS = {"Content-Type": "application/json", "Authorization": "Basic123"}
DEFAULT_WORK_POOL_NAME = "default"
DEFAULT_WORK_QUEUE_NAME = "default"



def load_yaml(file_path):
    """Load the YAML configuration file."""
    try:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as exc:
        logger.error(f"Error loading YAML file: {exc}")
        raise


def get_all_flows():
    """Retrieve all flows from the server."""
    url = f"{API_BASE_URL}/flows/filter"
    response = requests.post(url, headers=HEADERS, json={})
    response.raise_for_status()
    flows = response.json()
    logger.debug(f"Retrieved flows: {flows}")
    return {flow["name"]: flow for flow in flows}


def get_all_deployments():
    """Retrieve all deployments from the server."""
    url = f"{API_BASE_URL}/deployments/filter"
    response = requests.post(url, headers=HEADERS, json={})
    response.raise_for_status()
    deployments = response.json()
    logger.debug(f"Retrieved deployments: {json.dumps(deployments, indent=4)}")
    return {dep["name"]: dep for dep in deployments}


def create_or_update_flow(flow_name):
    """Ensure a flow exists on the server."""
    url = f"{API_BASE_URL}/flows/"
    logger.info(f"Creating or ensuring existence of flow: {flow_name}")
    response = requests.post(url, headers=HEADERS, json={"name": flow_name})
    response.raise_for_status()
    flow_id = response.json()["id"]
    logger.debug(f"Flow '{flow_name}' created or retrieved with ID: {flow_id}")
    return flow_id


def get_flow_name_by_id(flow_id):
    """
    Retrieve the flow name using its ID.
    """
    url = f"{API_BASE_URL}/flows/{flow_id}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    flow_data = response.json()
    return flow_data["name"]


def validate_and_transform_schedule_field(deployment):
    """
    Validate and transform the schedule or schedules field from the YAML or API response.
    Handles both single 'schedule' and multiple 'schedules' definitions.
    """
    transformed_schedules = []

    # Handle "schedules" key in the deployment (multiple schedules)
    if "schedules" in deployment and isinstance(deployment["schedules"], list):
        for schedule in deployment["schedules"]:
            if isinstance(schedule, dict):

                if "schedule" in schedule:  # deployment from API
                    transformed_schedule = {
                        "active": schedule.get("active", True),
                        "schedule": {
                            **(
                                {"cron": schedule["schedule"]["cron"]}
                                if "cron" in schedule.get("schedule")
                                else {}
                            ),
                            **(
                                {"interval": schedule["schedule"]["interval"]}
                                if "interval" in schedule.get("schedule")
                                else {}
                            ),
                            "timezone": schedule["schedule"].get(
                                "timezone", "Europe/Zurich"
                            ),
                        },
                        "catchup": schedule.get("catchup", False),
                    }
                    transformed_schedules.append(transformed_schedule)

                else:

                    transformed_schedule = {
                        "active": schedule.get("active", True),
                        "schedule": {
                            **(
                                {"cron": schedule["cron"]} if "cron" in schedule else {}
                            ),
                            **(
                                {"interval": schedule["interval"]}
                                if "interval" in schedule
                                else {}
                            ),
                            "timezone": schedule.get("timezone", "Europe/Zurich"),
                        },
                        "catchup": schedule.get("catchup", False),
                    }
                    transformed_schedules.append(transformed_schedule)

    # Handle "schedule" key in the deployment (single schedule)
    elif "schedule" in deployment and isinstance(deployment["schedule"], dict):
        schedule = deployment["schedule"]
        transformed_schedule = {
            "active": deployment.get(
                "is_schedule_active", True
            ),  # Handle the `is_schedule_active` flag
            "schedule": {
                **(
                    {"cron": schedule["cron"]}
                    if schedule.get("cron") is not None
                    else {}
                ),
                **(
                    {"interval": schedule["interval"]}
                    if schedule.get("interval") is not None
                    else {}
                ),
                "timezone": schedule.get("timezone", "Europe/Zurich"),
            },
            "catchup": schedule.get("catchup", False),
        }
        transformed_schedules.append(transformed_schedule)

    return transformed_schedules


def generate_openapi_schema(parameters):
    """
    Generate a parameter_openapi_schema for a deployment based on its parameters.
    """
    schema = {"type": "object", "properties": {}, "required": []}

    for param_name, param_value in parameters.items():
        param_type = get_openapi_type(param_value)
        schema["properties"][param_name] = {"type": param_type}

        # Mark the parameter as required if its value is not None
        if param_value is not None:
            schema["required"].append(param_name)

    # Remove 'required' if it's empty
    if not schema["required"]:
        schema.pop("required")

    return schema


def get_openapi_type(value):
    """
    Map Python types to OpenAPI types.
    """
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "number"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return "string"  # Default to string for unsupported types


def normalize_deployment_for_comparison(deployment, flow_name=None):
    """
    Normalize a deployment's structure for consistent comparison.
    Handles missing fields and formats nested fields as needed.
    """
    schedules = validate_and_transform_schedule_field(deployment)

    return {
        "name": deployment.get("name"),
        "flow_name": flow_name or deployment.get("flow_name"),
        "entrypoint": deployment.get("entrypoint"),
        "description": deployment.get("description"),
        "parameters": deployment.get("parameters", {}),
        "tags": sorted(deployment.get("tags", [])),
        "work_pool_name": deployment.get("work_pool_name", DEFAULT_WORK_POOL_NAME),
        "work_queue_name": deployment.get("work_queue_name", DEFAULT_WORK_QUEUE_NAME),
        "pull_steps": deployment.get("pull_steps", []),
        "schedules": schedules,
        "job_variables": deployment.get("job_variables", {})
    }


def compare_deployments(existing, new):
    """
    Compare two normalized deployment dictionaries and return differences.
    """
    changes = {}
    for key, new_value in new.items():
        existing_value = existing.get(key)
        if isinstance(new_value, list):  # For lists, compare sorted versions
            if sorted(existing_value or [], key=str) != sorted(new_value, key=str):
                changes[key] = {"existing": existing_value, "new": new_value}
        elif isinstance(new_value, dict):  # For dictionaries, compare recursively
            if existing_value != new_value:
                changes[key] = {"existing": existing_value, "new": new_value}
        else:
            if existing_value != new_value:
                changes[key] = {"existing": existing_value, "new": new_value}

    return changes


def create_or_update_deployment(normalized_deployment, flow_id):
    """
    Create or update a deployment with the Prefect REST API using normalized deployment data.
    """
    normalized_deployment.pop("flow_name")

    # Generate parameter_openapi_schema if parameters are present and not already defined
    if (
        normalized_deployment.get("parameters")
        and "parameter_openapi_schema" not in normalized_deployment
    ):
        normalized_deployment["parameter_openapi_schema"] = generate_openapi_schema(
            normalized_deployment["parameters"]
        )
        logger.debug(
            f"Generated parameter_openapi_schema for deployment '{normalized_deployment['name']}': {json.dumps(normalized_deployment['parameter_openapi_schema'], indent=2)}"
        )

    try:
        # Add the flow ID to the normalized deployment data
        normalized_deployment["flow_id"] = flow_id

        url = f"{API_BASE_URL}/deployments/"

        response = requests.post(url, headers=HEADERS, json=normalized_deployment)

        if response.status_code == 422:
            logger.error("Validation error when creating/updating deployment.")
            logger.error(
                f"Deployment data: {json.dumps(normalized_deployment, indent=2)}"
            )
            logger.error(f"Error details: {response.json()}")
            return

        response.raise_for_status()
        logger.info(f"Deployment '{normalized_deployment['name']}' created or updated.")
        logger.debug(
            f"Deployment data sent: {json.dumps(normalized_deployment, indent=2)}"
        )
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Failed to create or update deployment '{normalized_deployment['name']}': {e}"
        )
        raise


def delete_deployment(deployment_id, deployment_name):
    """Delete a deployment."""
    url = f"{API_BASE_URL}/deployments/{deployment_id}"
    logger.info(f"Deleting deployment '{deployment_name}' with ID: {deployment_id}")
    response = requests.delete(url, headers=HEADERS)
    response.raise_for_status()
    logger.debug(f"Deployment '{deployment_name}' deleted successfully.")


def synchronize_deployments(yaml_file):
    """
    Synchronize deployments and flows between the YAML file and the server.
    """
    yaml_data = load_yaml(yaml_file)
    yaml_deployments = {dep["name"]: dep for dep in yaml_data["deployments"]}
    yaml_flows = {dep["flow_name"] for dep in yaml_data["deployments"]}

    server_flows = get_all_flows()
    server_deployments_raw = get_all_deployments()
    server_deployments = {}

    # Enrich server deployments with flow names
    for dep_name, deployment in server_deployments_raw.items():
        flow_name = get_flow_name_by_id(deployment["flow_id"])
        normalized_deployment = normalize_deployment_for_comparison(
            deployment, flow_name
        )
        server_deployments[dep_name] = normalized_deployment

    # Synchronize flows and deployments
    for deployment_name, deployment in yaml_deployments.items():
        flow_name = deployment["flow_name"]

        # Ensure the flow exists
        if flow_name not in server_flows:
            logger.info(f"Flow '{flow_name}' not found. Creating it...")
            flow_id = create_or_update_flow(flow_name)
            server_flows[flow_name] = {"id": flow_id}
        else:
            logger.info(f"Flow '{flow_name}' exists. Using it...")
            flow_id = server_flows[flow_name]["id"]

        # Normalize YAML deployment
        normalized_yaml_deployment = normalize_deployment_for_comparison(deployment)

        # Compare and update/create deployment
        if deployment_name in server_deployments:
            changes = compare_deployments(
                server_deployments[deployment_name], normalized_yaml_deployment
            )
            if changes:
                logger.info(
                    f"Updating deployment '{deployment_name}' with changes: {json.dumps(changes, indent=4)}"
                )
                create_or_update_deployment(normalized_yaml_deployment, flow_id)
            else:
                logger.info(f"Deployment '{deployment_name}' is up-to-date.")
        else:
            logger.info(
                f"Creating deployment '{deployment_name}' for flow '{flow_name}'..."
            )
            create_or_update_deployment(normalized_yaml_deployment, flow_id)

    # Remove server deployments not in the YAML file
    for deployment_name, server_deployment in server_deployments_raw.items():
        if deployment_name not in yaml_deployments:
            delete_deployment(server_deployment["id"], deployment_name)

    # Delete flows not in the YAML file
    for flow_name, flow_details in server_flows.items():
        if flow_name not in yaml_flows:
            delete_flow(flow_details["id"], flow_name)


def delete_flow(flow_id, flow_name):
    """Delete a flow."""
    url = f"{API_BASE_URL}/flows/{flow_id}"
    logger.info(f"Deleting flow '{flow_name}' with ID: {flow_id}")
    response = requests.delete(url, headers=HEADERS)
    if response.status_code == 404:
        logger.warning(
            f"Flow '{flow_name}' not found on the server. It may have been already deleted."
        )
    else:
        response.raise_for_status()
        logger.info(f"Flow '{flow_name}' deleted successfully.")


if __name__ == "__main__":
    yaml_file_path = "deployments.yml"

    synchronize_deployments(yaml_file_path)
