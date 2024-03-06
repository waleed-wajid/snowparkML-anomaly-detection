# Import python packages
import streamlit as st
import plotly.express as px

from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import registry
from snowflake.ml.modeling.metrics import accuracy_score

import joblib
import pandas as pd



db = 'DEV_ML_WALEED'
schema = 'PROD'
TABLE_NAME = f"{db}.{schema}.PREDICTIONS"

# Get the current credentials
session = get_active_session()

# get data
df = session.table(TABLE_NAME)
pd_df = df.to_pandas()
pd_df.sort_values(by='EVENT_TIME', ascending=False, inplace=True)

# Convert column values to meaningful categories
for col in ['PRED_IS_ANOMALY', 'IS_ANOMALY']:
    pd_df.loc[pd_df[col] == -1, col] = 'Yes'
    pd_df.loc[pd_df[col] == 1, col] = 'No'
    
y_true = pd_df['IS_ANOMALY']
y_pred = pd_df['PRED_IS_ANOMALY']

# Write directly to the app
st.title("Anomaly Detection App")
st.write(
    """
    Displays anomalies detected using the
    Isolation forest model
    """
)

tab1, tab2 = st.tabs(['Data', 'Model'])

with tab1:
    st.subheader("Data over time")
    # Time-series plot
    fig = px.scatter(pd_df, x='EVENT_TIME', y='EVENT_VALUE',  color='PRED_IS_ANOMALY',
     color_discrete_sequence=['blue', 'red'])
    st.plotly_chart(fig)
    # Display prediction data
    st.subheader("Underlying data")
    st.dataframe(pd_df, use_container_width=True)

with tab2:
    # Display models in registry
    model_name =  "ANOMALY_DETECTION_ISOLATION_FOREST"
    native_registry = registry.Registry(session=session, database_name=db, schema_name='RAW')
    models_df = native_registry.get_model(model_name).show_versions()
    st.header("Models available")
    st.text("Table below shows the models the in the model registry")
    st.dataframe(models_df)

    # calculate and display accuracy
    acc = accuracy_score(df=df, y_true_col_names='IS_ANOMALY', y_pred_col_names='PRED_IS_ANOMALY')

    st.subheader('Model Performance')
    st.text(f"Model accuracy is {acc*100:.1f}%")
    st.text("The confusion matrix below shows the accuracy of the model:")
    # Confusion matrix of the model
    cm_fig = px.imshow(pd.crosstab(y_true, y_pred), text_auto=True)
    st.plotly_chart(cm_fig)

# snowflake_environment = session.sql('SELECT current_user(), current_version()').collect()

# # Current Environment Details
# st.text('\nConnection Established with the following parameters:')
# st.text('User                        : {}'.format(snowflake_environment[0][0]))
# st.text('Role                        : {}'.format(session.get_current_role()))
# st.text('Database                    : {}'.format(session.get_current_database()))
# st.text('Schema                      : {}'.format(session.get_current_schema()))
# st.text('Warehouse                   : {}'.format(session.get_current_warehouse()))
# st.text('Snowflake version           : {}'.format(snowflake_environment[0][1]))
