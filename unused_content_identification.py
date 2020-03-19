from looker_sdk import client, models
from looker_sdk.rtl import transport
import configparser
import json
import sys
import csv
from pprint import pprint

config_file = "looker.ini"
sdk = client.setup(config_file)

def get_base_url():
    """ Pull base url from looker.ini, remove port """
    config = configparser.ConfigParser()
    config.read(config_file)
    full_base_url = config.get("Looker", "base_url")
    base_url = sdk.auth.settings.base_url[:full_base_url.index(":19999")]
    return base_url

def join_content(match_list, left_key, iterator, right_key):
    """ Given a list of sdk objects and a list of dictionaries,
    join on a common value """
    joined_data = next(
        i for i in match_list if str(getattr(i,left_key)) == str(iterator[right_key])
    )
    return joined_data


def get_unused_content(days):
    """ Return unused content given a user-defined days since last accessed """
    unused_query = models.WriteQuery(
        model="system__activity",
        view="content_usage",
        fields=[
            "dashboard.id",
            "look.id",
            "dashboard.created_date",
            "look.created_date",
            "content_usage.content_title",
            "content_usage.content_type",
            "content_usage.embed_total",
            "content_usage.api_total",
            "content_usage.favorites_total",
            "content_usage.schedule_total",
            "content_usage.other_total",
            "content_usage.last_accessed_date"
        ],
        pivots=None,
        fill_fields=None,
        filters={
            "content_usage.days_since_last_accessed": f">{days}",
            "content_usage.content_type": "dashboard,look",
            "dashboard.deleted_date": "NULL",
            "look.deleted_date": "NULL"
        },
        filter_expression="NOT(is_null(${dashboard.id}) AND is_null(${look.id}))",
        sorts=["content_usage.other_total"],
        limit=None
    )

    unused_content = json.loads(sdk.run_inline_query(
        body=unused_query,
        result_format="json"
    ))
    return unused_content

def write_content_to_csv(content, output_csv_name):
    """Export new content errors in dev branch to csv file"""
    try:
        with open(output_csv_name, "w") as csvfile:
            writer = csv.DictWriter(
                csvfile,
               fieldnames=list(content[0].keys())
            )
            writer.writeheader()
            for data in content:
                writer.writerow(data)
        print(f"Content information outputed to {output_csv_name}")
    except IOError:
        print("I/O error")

def main():
    days = 90
    dashboard_keys = ["id", "title", "user_id", "folder"]
    look_keys = ["id", "title", "user_id", "folder"]
    user_keys = ["id", "first_name", "last_name", "email"]
    folder_keys = ["id", "parent_id", "name"]

    unused_content = get_unused_content(days)
    dashboards = sdk.all_dashboards(
        fields=", ".join(dashboard_keys),
        transport_options=transport.TransportSettings(timeout=600))
    looks = sdk.all_looks(
        fields=", ".join(look_keys),
        transport_options=transport.TransportSettings(timeout=600))
    users = sdk.all_users(
        fields=", ".join(user_keys),
        transport_options=transport.TransportSettings(timeout=600))
    folders = sdk.all_folders(
        fields=", ".join(folder_keys),
        transport_options=transport.TransportSettings(timeout=600))
    base_url = get_base_url()

    output_data = []
    for item in unused_content:
        row = {}
        row["dashboard_id"] = item["dashboard.id"]
        row["look_id"] = item["look.id"]
        row["dashboard_created_date"] = item["dashboard.created_date"]
        row["look_created_date"] = item["look.created_date"]
        row["content_title"] = item["content_usage.content_title"]
        row["content_type"] = item["content_usage.content_type"]
        row["last_accessed_date"] = item["content_usage.last_accessed_date"]
        if item["content_usage.content_type"] == "dashboard":
            try:
                dashboard = join_content(
                    dashboards, "id", item, "dashboard.id",
                )
                user_id = dashboard.user_id
                folder = dashboard.folder
                folder_id = folder.id
                folder_name = folder.name
                parent_folder_id = folder.parent_id
            except (StopIteration, AttributeError) as e:
                user_id = None
                folder = None
                folder_id = None
                folder_name = None
                parent_folder_id = None

        elif item["content_usage.content_type"] == "look":
            try:
                look = join_content(
                    looks, "id", item, "look.id"
                )
                user_id = look.user_id

                folder = look.folder
                folder_id = folder.id
                folder_name = folder.name
                parent_folder_id = folder.parent_id
            except (StopIteration, AttributeError) as e:
                user_id = None
                folder = None
                folder_id = None
                folder_name = None
                parent_folder_id = None
        row["user_id"] = str(user_id)
        row["folder_id"] = folder_id
        row["folder_name"] = folder_name
        row["parent_folder_id"] = parent_folder_id
        try:
            user= next(
                i for i in users if str(getattr(i,"id")) == str(user_id)
            )
            row["first_name"] = user.first_name
            row["last_name"] = user.last_name
            row["email"] = user.email
        except (StopIteration, KeyError) as e:
            row["first_name"] = None
            row["last_name"] = None
            row["email"] = None

        if row["content_type"] == "dashboard":
            id = row["dashboard_id"]
        else:
            id = row["look_id"]
        content_type = row["content_type"]

        row["url"] =  f"{base_url}/{content_type}s/{id}"
        try:
            parent_folder = join_content(
                folders, "id", row, "parent_folder_id"
            )
            row["parent_folder_name"] = parent_folder.name
        except (StopIteration, KeyError):
            row["parent_folder_name"] = None
        output_data.append(row)
    write_content_to_csv(output_data, "unused_content.csv")
main()
