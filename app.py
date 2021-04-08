import streamlit as st
import pandas as pd
from PIL import Image
import glob
from src.utils import *

# App UI
st.set_page_config(
    page_title="Open Targets Data Metrics",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("A sneak peek at the stats in Open Targets")
st.markdown("This application is a dashboard to display the metrics of the different data releases in Open Targets.")
st.sidebar.title("What do you want to check?")

# Load data
files = glob.glob("data/*.csv")
dfs = [pd.read_csv(f) for f in files]
data = pd.concat(dfs, ignore_index=True)

# Post or pre pipeline run metrics
select = st.sidebar.checkbox("Pre pipeline metrics")
if select:
    st.subheader('Metrics of the data ran before the pipeline run.')
    data = data[data["runId"].str.contains("pre")]

pivot = pd.pivot_table(data, values="count", index=["variable", "field"], columns="datasourceId")
st.write(pivot)

st.sidebar.title("Choose a pipeline run")
release = st.sidebar.selectbox(
    'Data release',
    data.runId.unique()
)

st.sidebar.title("Select a variable of interest")
variable = st.sidebar.selectbox(
    "Select variable",
    data.variable.unique()
)

# Create filters
release_mask = data["runId"] == release
variable_mask = data["variable"] == variable

# Display table
filtered_df = data[release_mask & variable_mask]
pivot = pd.pivot_table(filtered_df, values="count", index=["variable", "field"], columns="datasourceId")
st.write(pivot)
st.markdown(get_table_download_link_csv(data), unsafe_allow_html=True)


st.image(Image.open("img/OT logo.png"), width = 150)