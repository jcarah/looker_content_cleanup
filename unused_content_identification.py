import looker_sdk
from looker_sdk import models
import configparser
import json
import sys
import csv
from pprint import pprint

config_file = "sandbox.ini"
sdk = looker_sdk.init31(config_file)

def get_base_url():
    """ Pull base url from looker.ini, remove port """
    config = configparser.ConfigParser()
    config.read(config_file)
    base_url = config.get("Looker", "base_url").replace(":19999", '')
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
        limit=10000
    )
    unused_content = json.loads(sdk.run_inline_query(
        body=unused_query,
        result_format="json"
    ))
    return unused_content

def get_dashboard_models(dashboard_ids):
    """Collect metadata about dashboards"""
    dashboard_query = models.WriteQuery(
        model="system__activity",
        view="dashboard",
        fields=["query.model", "dashboard.id"],
        filters={
            "dashboard.id" : ','.join(dashboard_ids)
        },
        limit=10000
    )
    dashboard_info = json.loads(sdk.run_inline_query(
        body=dashboard_query,
        result_format="json"
    ))
    return dashboard_info

def get_look_models(look_ids):
    """Collect model info about looks"""
    look_query = models.WriteQuery(
        model="system__activity",
        view="look",
        fields=["query.model", "look.id"],
        filters={
            "look.id" : ','.join(look_ids)
        },
        limit=10000
    )
    look_info = json.loads(sdk.run_inline_query(
        body=look_query,
        result_format="json"
    ))
    return look_info

def flatten_content(content_type, content):
    """collects all the models associated with a given look/dashboard"""
    obj = {}
    content_id = f'{content_type}.id'
    for item in content:
        try:
            if item[content_id] in obj.keys():
                obj[item[content_id]]['query.model'].append(item['query.model'])
            else:
                obj[item[content_id]] = {
                    "query.model": [item['query.model']]
                }
        except:
            pass
    ret_arr = []
    for id, item in obj.items():
        ret_arr.append({
            content_id: id,
            "query.models": item['query.model']
        })
    return ret_arr

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
    if unused_content:
        dashboards = sdk.all_dashboards(
            fields=", ".join(dashboard_keys))
        looks = sdk.all_looks(
            fields=", ".join(look_keys))
        users = sdk.all_users(
            fields=", ".join(user_keys))
        folders = sdk.all_folders(
            fields=", ".join(folder_keys))
        base_url = get_base_url()
        output_data = []
        for item in unused_content:
            row = {}
            row["dashboard_id"]  = item.get("dashboard.id", None)
            row["look_id"]  = item.get("look.id", None)
            row["dashboard_created_date"] = item.get("dashboard.created_date", None)
            row["look_created_date"] = item.get("look.created_date", None)
            row["content_title"] = item.get("content_usage.content_title", None)
            row["content_type"] = item.get("content_usage.content_type", None)
            row["last_accessed_date"] = item.get("content_usage.last_accessed_date", None)
            content_type = item.get("content_usage.content_type", None)
            if content_type == "dashboard":
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

            elif content_type == "look":
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
        unused_look_ids = [
            str(i['look_id']) for i in output_data if i['look_id'] is not None
        ]
        unused_dashboard_ids = [
            str(i['dashboard_id']) for i in output_data if i['dashboard_id'] is not None
        ]
        look_models = flatten_content('look',get_look_models(unused_look_ids)) 
        dashboard_models = flatten_content('dashboard', get_dashboard_models(unused_dashboard_ids))
        for item in output_data:
            if item['content_type'] == 'dashboard':
                try:
                    models = next(
                        i for i in dashboard_models if item['dashboard_id'] == i['dashboard.id']
                    )['query.models']
                    item['models'] = ', '.join(filter(None, models))
                except:
                    pass
            else:
                try:
                    models = next(
                        i for i in look_models if item['look_id'] == i['look.id']
                    )['query.models']
                    item['models'] = ', '.join(filter(None, models))
                except:
                    pass
        write_content_to_csv(output_data, "unused_content.csv")
    else:
        print("No unused content.")
main()
