
# Anomaly Detection using Isolation Forests in SnowparkML
## Overview
This project shows how SnowparkML can be leveraged to a) train an Isolation Forest model for detecting anomalies and b) deploy the model in Snowflake to run inferences on new data uploaded on a schedule.

## Requirements
* Snowflake account with `SYSADMIN` privileges. If you don't have one you can setup a free [trial account](https://signup.snowflake.com/).
* Local envrionment with anoconda/ miniconda installed. Miniconda can be installed from [here](https://conda.io/miniconda.html). Python 3.11 and venv can be used alternatively.

## Set-up
Clone this repository to your local development environment using:

```
git clone https://github.com/waleed-wajid/snowparkML-anomaly-detection.git
```

Navigate to the cloned repo:
```
 cd snowparkML-anomaly-detection
 ```

### Snowflake
Update the `connection.json` file with the connection params for your snowflake instance. 

To setup the database and schemas required for this project copy all statements from `./sql/01-setup.sql` into a SQL worksheet in snowsight and execute all. (alternatively you can use [SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql]) to run the script from your terminal).

### Python/ conda environment
From the terminal in your local development environment run:
```
conda env create -f conda_env.yml
```
This will create an isolated python environment for this project. Activate the environment with:
``` 
conda activate snowpark-ml-dq
```

## Step-by-step 
1. Run all the cells in the `notebooks/01-SnowparkML-AnomalyDetection.ipynb` notebook to generate a toy dataset, train and register a model.
2. Run the `sql/02-sp-data-update.sql` script in Snowflae to create stored proc to update sample data in table.
3. Run the `sql/03-sp-run-model-inference.sql` script to create stored proc to run model inference on new data.
4. Run the `sql/04-tasks-upload-data-and-run-model.sql` script to create 2 tasks to update data on schedule and run inference after. 
5. (Optional) Create a [Streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) app from the Snowsight UI and copy the code from `streamlit/app.py` and run it to display the data and results from the model.
