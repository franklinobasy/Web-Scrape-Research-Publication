from publication import Pipeline

pipeline = Pipeline(
    pipeline_name="my_pipeline",
    export_path="publication.json",
    connection_param={
        "user": "",
        "password": "",
        "database": "",
        "port": "",
        "host": ""
    },
    table_name="publication"
)

pipeline.execute()
