from looker_sdk import client, models
from looker_sdk.rtl import transport

config_file = 'looker.ini'
sdk = client.setup(config_file)


def soft_delete_dashboard(dashboard_id):
    dashboard = models.WriteDashboard(deleted=True)
    try:
        sdk.update_dashboard(dashboard_id, body=dashboard)
        print(f"Successfully soft deleted dashboard {dashboard_id}")
    except Exception as e:
        print(f"Error: {e}")

def soft_delete_look(look_id):
    look = models.WriteLookWithQuery(deleted=True)
    try:
        sdk.update_look(look_id, body=look)
        print(f"Successfully soft deleted dashboard {look_id}")
    except Exception as e:
        print(f"Error: {e}")

# Provide a list of look_ids to soft delete
looks_to_delete = []

for look in looks_to_delete:
    soft_delete_look(look)

# Provide a list of dashboard_ids to soft delete
dashboards_to_delete = []

for dashboard in dashboards_to_delete:
    soft_delete_dashboard(dashboard)
