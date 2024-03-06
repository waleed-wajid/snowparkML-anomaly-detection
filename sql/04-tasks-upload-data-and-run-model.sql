/*
This script does the following:
1. creates a task to call the load data SP everyday. 
2. Creates a stream on the uploaded data.
3. Creates a second task that calls the model inference SP after the load data task on new data in the stream.
 */
use role SYSADMIN;
use warehouse DEV;

use database DEV_ML_WALEED;
use schema raw;


-- 1. create task for data upload every morning at 5AM AEST / 1800 UTC
CREATE OR REPLACE TASK sample_data_upload_task_WA
  WAREHOUSE = DEV
  SCHEDULE = 'USING CRON 0 18 * * * UTC'
  COMMENT = 'Task to upload sample data to the timeseries table every morning'
  AS
    CALL sp_update_data('TIMESERIES')
  ;

-- 2. enable change tracking to create a stream
alter table RAW.TIMESERIES set CHANGE_TRACKING = true; 

-- create stream on the table to get new records
create or replace stream TIMESERIES_STREAM on table RAW.TIMESERIES SHOW_INITIAL_ROWS=true;
select count(*) from TIMESERIES_STREAM;
select * from TIMESERIES_STREAM limit 10;


-- 3. Create a task to run inference after data has been loaded
create or replace task run_inference_WA
WAREHOUSE=DEV
COMMENT='Task to run inference using anomaly detection model after new data is loaded'
as
call sp_run_inference('PREDICTIONS', 'PROD', 'TIMESERIES_STREAM', 'RAW');

alter task run_inference_WA add after sample_data_upload_task_WA;
alter task run_inference_WA resume;
alter task sample_data_upload_task_WA resume;

-- check if tasks created successfully
show tasks;