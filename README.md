# ETL Pipeline

This package contains functionalities that extracts, transfroms, and load data from [Coventry University's School of Economics, Finance and Accounting](https://pureportal.coventry.ac.uk/en/organisations/school-of-economics-finance-and-accounting/publications/) to a postgreSQL database.

Here is the structure of the package's architecture:

```shell
.
├── README.md
├── main.py  
└── publication     
    ├── __init__.py 
    ├── extract.py  
    ├── load.py     
    ├── pipeline.py 
    └── transform.py

1 directory, 7 files
```

The publication package consists of 4 important modules:

- [`extract.py`](publication/extract.py): This module uses web scraping tools to exract data from the website to a json file
- [`transform.py`](publication/transform.py): This module uses pandas library to read the json file to a pandas dataframe
- [`load.py`](publication/load.py): This module loads the dataframe to a postgreSQL database
- [`pipeline.py`](publication/pipeline.py): This module simulates a pipeline that creates an ETL workflow using [`extract.py`](publication/extract.py), [`transform.py`](publication/transform.py), [`load.py`](publication/load.py).

To create a `pipeline`:

1. Import the `Pipeline` class:

```python
from publication import Pipeline
```

2. Instantiate the `pipeline` from the `Pipeline` class using the following arguments:

  - `pipeline_name`: Custom or user-defined name for the pipelines. This helps you to distinguish different instance of the `Pipeline`
  
  - `export-path`: The path to save the `json` file generated from the program. This path should contain a valid `json` file
  
  - `connection-param`: A dictionary of the database credentials:
    - `user`: postgres user name
    - `password`: user's password
    - `database`: name of the database
    - `port`: connection port
    - `host`: database host
  
  - `table_name`: name of the table to use in the database

```python
pipeline = Pipeline(
    pipeline_name="my_pipeline",
    export_path="example.json",
    connection_param={
        "user": "",
        "password": "",
        "database": "",
        "port": "",
        "host": ""
    },
    table_name="example"
)
```

3. Execute the `pipeline`:

```python
pipeline.execute()
```
