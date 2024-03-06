/*
Script to create a stored proc to run the ML model and upload results.
 */
use role SYSADMIN;
use warehouse DEV;
use database DEV_ML_WALEED;
use schema RAW;

create or replace procedure sp_run_inference(output_tbl VARCHAR, output_schema VARCHAR, input_table VARCHAR, input_schema VARCHAR)
  returns string
  language python
  runtime_version = '3.11'
  packages = ('snowflake-snowpark-python', 'pandas', 'snowflake-ml-python')
  handler = 'run'
as
$$
import pandas as pd
import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.version import VERSION
from snowflake.ml.registry import registry
import joblib

def run(session, output_tbl, output_schema, input_table, input_schema):
    """
    fucntion loads data from input_schema.input_table, runs the anomaly detection model on it.
    Uploads predictions to output_schema.output_tbl.

    Parameters:
    --------------
    session: Snowpark Session object
    output_tbl: name of output table
    output_schmea: name of output schema
    input_table: name of input (feature) table
    input_schema: name of input schema to load data from
    """
    db = 'DEV_ML_WALEED'
    schema = input_schema
    TABLE_NAME = f"{db}.{schema}.{input_table}"

    # load model and preprocessing pipeline
    preprocessing_pipeline = session.file.get("@ML_ASSETS/preprocessing_pipeline.joblib.gz", target_directory='/tmp')
    preprocessing_pipeline = joblib.load(f"/tmp/{preprocessing_pipeline[0].file}") #preprocessing_pipeline[0].file
    preprocessing_pipeline

    model_name =  "ANOMALY_DETECTION_ISOLATION_FOREST"
    native_registry = registry.Registry(session=session, database_name=db, schema_name=schema)
    model_if = native_registry.get_model(model_name).version('v0')

    df = session.table(TABLE_NAME).select(col('EVENT_TIME'), col('EVENT_VALUE'), col('IS_ANOMALY'))
    result = model_if.run(preprocessing_pipeline.transform(df), function_name='predict')

    session.write_pandas(result.to_pandas(), output_tbl, schema=output_schema, auto_create_table=True, overwrite=False, use_logical_type=True)

    return "SUCCESS: data uploaded"

$$;
