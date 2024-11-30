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

def load_automation_file(filepath: str) -> dict:
    """Load automations.yaml from the repository."""
    with open(filepath, "r") as file:
        automations = yaml.safe_load(file)
    return automations

def fetch_automation(automation_name: str) -> dict:
    """Fetch an automation by name from the Prefect server."""
    automation_url = f"{PREFECT_API_URL}/automations/name/{automation_name}"
    response = requests.get(automation_url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    return None

def compare_automations(local_automation: dict, server_automation: dict) -> bool:
    """Compare the automation data with the server version and return True if they differ."""
    fields_to_compare = ['name', 'description', 'enabled', 'trigger', 'actions']
    
    for field in fields_to_compare:
        if local_automation.get(field) != server_automation.get(field):
            return True  # Differences found, an update is needed
    return False  # No differences found, no update needed

def create_or_update_automation(automation_data: dict):
    """Create or update an automation using the Prefect REST API."""
    automation_name = automation_data.get('name')

    # Fetch the existing automation from the server, if it exists
    server_automation = fetch_automation(automation_name)

    # Compare automations if server version exists
    if server_automation:
        if compare_automations(automation_data, server_automation):
            # Differences detected, update the automation
            automation_url = f"{PREFECT_API_URL}/automations/{server_automation['id']}"
            response = requests.put(automation_url, headers=HEADERS, json=automation_data)
            if response.status_code in [200, 201]:
                print(f"Automation {automation_name} updated successfully.")
            else:
                print(f"Failed to update automation {automation_name}: {response.text}")
        else:
            # No differences detected, skip update
            print(f"Automation {automation_name} is already up-to-date. No update necessary.")
    else:
        # Automation doesn't exist, create it
        automation_url = f"{PREFECT_API_URL}/automations"
        response = requests.post(automation_url, headers=HEADERS, json=automation_data)
        if response.status_code in [200, 201]:
            print(f"Automation {automation_name} created successfully.")
        else:
            print(f"Failed to create automation {automation_name}: {response.text}")

def delete_automation(automation_id: str):
    """Delete an automation using the Prefect REST API."""
    delete_url = f"{PREFECT_API_URL}/automations/{automation_id}"
    response = requests.delete(delete_url, headers=HEADERS)
    if response.status_code == 204:
        print(f"Successfully deleted automation with ID: {automation_id}")
    else:
        print(f"Failed to delete automation: {response.text}")

def list_automations() -> list:
    """List all automations using the Prefect REST API."""
    list_url = f"{PREFECT_API_URL}/automations"
    response = requests.get(list_url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch automations: {response.text}")
        return []

def compare_and_delete_old_automations(current_automations: list, yaml_automations: list):
    """Compare the automations on the server and YAML file, and delete any missing from YAML."""
    yaml_automation_names = {automation['name'] for automation in yaml_automations}

    # Find automations on the server that are not in the YAML file
    for automation in current_automations:
        if automation['name'] not in yaml_automation_names:
            print(f"Automation {automation['name']} is missing from YAML and will be deleted.")
            delete_automation(automation['id'])

def deploy_automations_from_yaml(filepath: str):
    """Deploy or update all automations from a YAML file, and delete removed automations."""
    # Load automations from YAML
    yaml_automations = load_automation_file(filepath).get('automations', [])

    # Deploy or update each automation from the YAML file
    for automation in yaml_automations:
        create_or_update_automation(automation)

    # Fetch existing automations from the Prefect server
    current_automations = list_automations()

    # Delete old automations that are no longer in the YAML file
    compare_and_delete_old_automations(current_automations, yaml_automations)

if __name__ == "__main__":
    # Example usage
    automations_file = "automations.yml"
    
    # Deploy all automations from YAML and remove old ones
    deploy_automations_from_yaml(automations_file)
