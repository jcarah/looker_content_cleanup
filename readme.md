# Warning

The last_accessed_date for Looks can be misleading. It refers to the last time they were accessed directly, not when they were last accessed on a dashboard. Please avoid deleting Looks. A future iteration of these scripts will exclude Look-linked dashboard tiles. 

# Looker Content Cleanup

A collection of scripts to help identify unused and broken content in Looker. In a future iteration, we'll include methods for removing such content.

### Prerequisites

These scripts rely on the new Looker [Python SDK](https://github.com/looker-open-source/sdk-codegen/tree/master/python), which requires Python 3.7+.

Additional required Python dependencies can be found requirements.txt, and can be installed with `pip`.

### Getting started

* Clone this repo, and configure a file called `looker.ini` in the same directory as the two Python scripts. Follow the instructions [here](https://github.com/looker-open-source/sdk-codegen/tree/master/python#configuring-the-sdk) for more detail on how to structure the `.ini` file. The docs also describe how to use environment variables for API authentication if you so prefer.
* Install all Python dependencies in `requirements.txt`

### Usage

- Running `python unused_content_identification.py` will pull down metadata about Looker content that hasn't been queried in 90 days and output the results in a file called `unused_content.csv`.
- Running `python broken_content_identification.py` will pull down metadata about Looker content that's broken, as well the content's last query date, and output the results in a file called `broken_content.csv`.
