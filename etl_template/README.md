# ETL Templating

This repository aims to deploy descriptions of logical data models and descriptions of model lineage mappings to fill those models with data for multiple technical solutions.

Be warned: this code is still far from the stated goal and currently just implements data model implementations using 'create schema' and 'create table' DDLs for [dedicated SQL pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-overview-what-is) and [duckdb](https://duckdb.org/).

The configuration for model input and templating can be adapted in ```config.yml```. The purpose of a making the directory for templates configurable is that we can add templates for multiple database implementations that each generate different DDL outputs.

* The bare-bones example theorethical model is described as a JSON in ```input/models.json```, but need to be replaced by PowerDesigner XML's. See the section [Power Designer LDM conversion](#Power Designer LDM conversion)
* The [Jinja templating engine](https://jinja.palletsprojects.com/en/stable/templates/) is used to generate implementations. Two example templates are added:
  * a create schema DDL template ```templates/{implementation}/create_schema.sql```
  * a create table DDL template ```templates/{implementation}/create_table.sql```
* The output is a file for each DDL written in the directory ```output/{implementation}```

## Getting started

* Clone the repository
* Create a virtual environment and add the libraries from ```requirements.txt```
* Run ```main.py```
* To run an example for a duckdb deployment you can run ```duckdb_deploy.py``` after you ran the duckdb example in main. The resulting database can be found in ```output/duckdb/duckdb.db```, which can be browsed with [dbeaver](https://duckdb.org/docs/guides/sql_editors/dbeaver.html).

### Power Designer LDM conversion

The current code is based on my own sample data structure, but we want to move to PowerDesigner generated model data. As a starting point the [example model](https://generate.x-breeze.com/docs/3.1/Examples/) documents from [CrossBreeze](https://crossbreeze.nl/) are added to the repository (```input/ExampleSource.ldm```, ```input/Reference.ldm``` and ```input/ExampleDWH.ldb```). The script ```pd_document.py``` is the entry point for extracting data into objects. This results in a JSON file ```output/ExampleDWH.json``` which should contain all the elements to deploy a model and model mapping data which can enable ETL. A start is made with the ```PDDocumentQuery``` class that can query this data for specific purposes (templating for example).

#### References
* [xmltodict](https://pypi.org/project/xmltodict/) is used to convert XML into Python [dictionaries](https://realpython.com/python-dicts/), which in turn can be written to a JSON file.
* Logs are written as JSON with [python-json-logger](https://pypi.org/project/python-json-logger/) in the terminal and to a file ```log.json``` using log rotation. The logging configuration can be changed in the file ```logging_config.py```.

## Future developments

* Align way of designing in PowerDesigner and ETL extraction for this script, so we ensure an understanding between the data modeller (business analist) and the Data Engineer. This [presentation](https://docs.google.com/presentation/d/e/2PACX-1vSz0YO-Zb-OxNcQNjBMmwl-HqMe3lqDiZ2mH8qlQZGwpCddTSVQRgFPpJm3Dkvh5JsThuhzpjZtZWUj/pub?start=false&loop=false&delayms=3000) on this is a work in progress.
* Create templates and procedures for testing on model constraints:
  * Entity identifier
  * Relationships
* Add more elaborate logging
* Added example docstrings and documentation generation as inspiration. Currently the simple [pydoc](https://docs.python.org/3/library/pydoc.html) is used, as the project extends we should consider switching to [Sphinx](https://www.sphinx-doc.org/en/master/)
* Antecedents and precedents reporting for escalation business processes using [graphs](https://python.igraph.org/en/latest/tutorial.html) and [graph visualizations](https://networkx.org/). [Tutorial](https://www.youtube.com/watch?v=D8zXTiOLrYA&ab_channel=PythonTutorialsforDigitalHumanities)
* [Merge loading for mappings](https://techcommunity.microsoft.com/blog/azuresynapseanalyticsblog/merge-t-sql-for-dedicated-sql-pools-is-now-ga/3634331)
