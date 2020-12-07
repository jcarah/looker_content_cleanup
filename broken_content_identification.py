import looker_sdk
from looker_sdk import models
import configparser
import csv
import json
from pprint import pprint

config_file = "looker3.ini"
sdk = looker_sdk.init31(config_file)

def main():
    base_url = get_base_url()
    space_data = get_space_data()
    content_usage = get_content_usage()
    print("Checking for broken content in production.")
    content_validator_output = get_broken_content()
    print("Parsing broken content")
    broken_content = parse_broken_content(
        base_url,
        content_validator_output,
        space_data,
        content_usage
    )
  
    print("Done checking for broken content")
    write_broken_content_to_file(broken_content, "broken_content.csv")

def get_base_url():
    """ Pull base url from looker.ini, remove port"""
    config = configparser.ConfigParser()
    config.read(config_file)
    full_base_url = config.get("Looker", "base_url")
    base_url = sdk.auth.settings.base_url[:full_base_url.index(":19999")]
    return base_url

def get_space_data():
    """Collect all spaces"""
    space_data = sdk.all_spaces(fields="id, parent_id, name")
    return space_data

def get_content_usage():
    """Collect usage stats for all content"""
    query = models.WriteQuery(
        model="system__activity",
        view="content_usage",
        fields=[
            "dashboard.id",
            "look.id",
            "content_usage.last_accessed_date",
            "_dashboard_linked_looks.is_used_on_dashboard"
        ],
        pivots=None,
        fill_fields=None,
        filters={
            "content_usage.content_type": "dashboard,look"
        },
        filter_expression="NOT(is_null(${dashboard.id}) AND is_null(${look.id}))",
        limit=None
    )
    unused_content = json.loads(sdk.run_inline_query(
        body=query,
        result_format="json"
    ))
    return unused_content

def get_broken_content():
    """Collect broken content"""
    broken_content  = sdk.content_validation(
        ).content_with_errors   
    return broken_content

def parse_broken_content(base_url, broken_content, space_data, content_usage):
    """Parse and return relevant data from content validator"""
    output = []
    for item in broken_content:
        if item is not None:
            if item.dashboard:
                content_type = "dashboard"
            else:
                content_type = "look"
            item_content_type = getattr(item, content_type)
            id = item_content_type.id
            name = item_content_type.title
            space_id = item_content_type.space.id
            space_name = item_content_type.space.name
            errors = item.errors
            url =  f"{base_url}/{content_type}s/{id}"
            space_url = "{}/spaces/{}".format(base_url,space_id)
            if content_type == "look":
                    element = None
            else:
                dashboard_element = item.dashboard_element
                element = dashboard_element.title if dashboard_element else None
            # Lookup additional space information
            try:
                space = join_content_sdk(space_data, "id", space_id)
                parent_space_id = space.parent_id
            except (StopIteration):
                parent_space_name = None
            # Old version of API  has issue with None type for all_space() call
            if  parent_space_id is None or parent_space_id == "None":
                parent_space_url = None
                parent_space_name = None
            else:
                parent_space_url = "{}/spaces/{}".format(
                    base_url,
                    parent_space_id
                )
                # Handling an edge case where space has no name. This can happen
                # when users are improperly generated with the API
                try:
                    parent_space = join_content_sdk(space_data, "id", parent_space_id)
                    parent_space_name = parent_space.name
                except (StopIteration):
                    parent_space_name = None
            if content_type == "dashboard":
                try:
                    usage = join_content_dict(content_usage, "dashboard.id", id)
                    last_accessed_date = usage["content_usage.last_accessed_date"]
                except Exception as e:
                    print(e)
                    last_accessed_date = None
            elif content_type == "look":
                try:
                    usage = join_content_dict(content_usage, "look.id", id)
                    last_accessed_date = usage["content_usage.last_accessed_date"]
                    is_dashboard_linked_look = ["_dashboard_linked_looks.is_used_on_dashboard"]
                except Exception as e:
                    print(e)
                    last_accessed_date = None
            else:
                last_accessed_date = None
            data = {
                    "id" : id,
                    "content_type" : content_type,
                    "name" : name,
                    "url" : url,
                    "dashboard_element": element,
                    "space_name" : space_name,
                    "space_url" : space_url,
                    "parent_space_name": parent_space_name,
                    "parent_space_url": parent_space_url,
                    "errors": str(errors),
                    "last_accessed_date": last_accessed_date,
                    "is_dashboard_linked_look": last_accessed_date
                }
            output.append(data)
            
    return output

def join_content_dict(match_list, left_key, right_key):
    """ Given a list of dictionaries and a local variable,
    join on a common value """
    joined_data = next(
        i for i in match_list if str(i[left_key]) == str(right_key)
    )
    return joined_data

def join_content_sdk(match_list, left_key, right_key):
    """ Given a list of sdk objects and a local variable,
    join on a common value """
    joined_data = next(
        i for i in match_list if str(getattr(i,left_key)) == str(right_key)
    )
    return joined_data

def write_broken_content_to_file(broken_content, output_csv_name):
    """Export content errors in dev branch to csv file"""
    try:
        with open(output_csv_name, "w") as csvfile:
            writer = csv.DictWriter(
                csvfile,
               fieldnames=list(broken_content[0].keys())
            )
            writer.writeheader()
            for data in broken_content:
                writer.writerow(data)
        print("Broken content information outputed to {}".format(
            output_csv_name
        ))
    except IOError:
        print("I/O error")

main()
