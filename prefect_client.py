import asyncio
import yaml
from prefect import get_client
import requests
import json

DEPLOYMENT_POSSIBLE_ATTRIBUTES = [
    "flow_id",
    "name",
    "version",
    "schedules",
    "description",
    "is_schedule_active",
    "parameters",
    "tags",
    "work_queue_name",
    "work_pool_name",
    "entrypoint",
    "pull_steps",
]


def normalize_value(value):
    """
    Converts string 'None' to Python None and returns the value as is for other cases.
    """
    if value == "None":
        return None
    return value


async def main():
    async with get_client(httpx_settings={}) as client:
        # 1. Get all deployments in the yaml file
        with open("deployments.yml", "r") as file:
            try:
                parsed_yaml = yaml.safe_load(file)
            except yaml.YAMLError as exc:
                print(f"Error parsing YAML file: {exc}")
                return

        yaml_flows = {
            deployment["flow_name"]: deployment
            for deployment in parsed_yaml["deployments"]
        }

        # 2. Get all flows on the server
        server_flows = await client.read_flows(limit=200, offset=0)
        server_flows = {flow.name: flow for flow in server_flows}

        # 3. Get all deployments on the server
        all_server_deployments = await client.read_deployments(limit=200, offset=0)

        server_deployments_dict = {
            deployment.name: dict(deployment) for deployment in all_server_deployments
        }

        for deployment_name in server_deployments_dict.keys():
            # Convert UUID object to string
            server_deployments_dict[deployment_name]["flow_id"] = str(
                server_deployments_dict[deployment_name]["flow_id"]
            )

        # 4. Check if each flow in the YAML exists on the server and create it if not
        # TODO: and DELETE if flow/deployment (combination) in server but not in yaml
        for flow_name, deployment in yaml_flows.items():

            if flow_name not in server_flows:
                # Create flow
                print(f"Flow '{flow_name}' not found on server. Creating it.")
                flow = await client.create_flow_from_name(flow_name=flow_name)
                flow_id = str(flow)
            else:
                # Get flow from server flow dict
                flow = server_flows[flow_name]
                flow_id = str(flow.id)

            yaml_deployment_representation = {
                "flow_id": flow_id,
                **{key: deployment[key] for key in ["name", "version", "description", "entrypoint", "pull_steps"] if key in deployment},
                **{key: deployment.get(key, []) for key in ["schedules", "tags"] if deployment.get(key)},
                **{key: deployment.get(key, False) for key in ["is_schedule_active"] if deployment.get(key) is not None},
                **{key: deployment.get(key, {}) for key in ["parameters"] if deployment.get(key)},
                "work_pool_name": deployment["work_pool"].get("name") if "work_pool" in deployment and "name" in deployment["work_pool"] else None,
                "work_queue_name": deployment["work_pool"].get("work_queue_name") if "work_pool" in deployment and "work_queue_name" in deployment["work_pool"] else None
            }



            # Check if the deployment already exists
            if deployment["name"] in server_deployments_dict:
                print(
                    f"Deployment '{deployment['name']}' already exists for flow '{flow_name}'. Cheking if update is needed.."
                )

                server_deployment_representation = server_deployments_dict[
                    deployment["name"]
                ]

                server_deployment_id = server_deployments_dict[deployment["name"]]["id"]

                # Filter dictionary to only include the keys from DEPLOYMENT_POSSIBLE_ATTRIBUTES
                server_deployment_representation = {
                    key: server_deployment_representation[key]
                    for key in DEPLOYMENT_POSSIBLE_ATTRIBUTES
                    if key in server_deployment_representation
                }

                yaml_deployment_representation.pop('flow_id')
                yaml_deployment_representation.pop('name')
                yaml_deployment_representation.pop('pull_steps')

                yaml_deployment_representation["job_variables"] = {}
                yaml_deployment_representation['enforce_parameter_schema'] = True

                # If flow_id differs, we need to create a new deployment since the PATCH method can not receive a flow_id (or name)

                headers = {"Content-Type": "application/json", "accept": "*/*"}

                # PATCH method
                print(
                    f"PATCH 'https://127.0.0.1:4200/api/deployments/{server_deployment_id}' with data:\n{json.dumps(yaml_deployment_representation, indent=4)}"
                )

                r = requests.patch(
                    f"http://127.0.0.1:4200/api/deployments/{server_deployment_id}",
                    data=json.dumps(yaml_deployment_representation),
                    headers=headers,
                )
                print(f"\nRESPONSE:\n{json.dumps(r.json(), indent=4)}")

            else:
                await client.create_deployment(**yaml_deployment_representation)
                print(
                    f"Deployment '{deployment['name']}' created for flow '{flow_name}'."
                )


# Run the main async function
asyncio.run(main())
