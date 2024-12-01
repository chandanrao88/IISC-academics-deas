import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
import time

# Google Cloud BigQuery setup
PROJECT_ID = "yteam-plutus-iisc"
TABLE_ID = "location.visited_location_view"
client = bigquery.Client.from_service_account_json('/path-to-json-key/team-plutus-iisc-a8b18655b592.json')

# Function to fetch data from BigQuery
def fetch_data():
    query = f"""
    SELECT lat_visit, lon_visit
    FROM `{TABLE_ID}`
    """
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    return df

# Streamlit app layout
st.set_page_config(page_title="Population Heatmap", layout="wide")
st.title("Live Population Heatmap")

# Map initialization
st.markdown("### Heatmap Visualization")

# Refresh interval input
refresh_interval = st.sidebar.number_input(
    "Refresh Interval (seconds)", min_value=5, max_value=60, value=10, step=5
)

# Initialize session state for caching data and map
if "last_data" not in st.session_state:
    st.session_state["last_data"] = None

# Fetch new data from BigQuery and create the map if not available in session or on manual refresh
data = fetch_data()

if data.empty:
    st.warning("No data found in the table.")
else:
    # Get the mean lat/lon for centering the map
    mean_lat = data["lat_visit"].mean()
    mean_lon = data["lon_visit"].mean()

    # Create the map dynamically based on lat/lon from the data
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=14)

    # Add points to the map
    for index, row in data.iterrows():
        folium.CircleMarker(
            location=(row["lat_visit"], row["lon_visit"]),
            radius=5,
            color="blue",
            fill=True,
            fill_opacity=0.7,
        ).add_to(m)

    # Display the map in Streamlit
    st_folium(m, width=700, height=500)

    # Store the last fetched data and map in session state
    st.session_state["last_data"] = data

# Trigger auto-refresh using a button
if st.button("Refresh Map"):
    st.session_state["last_data"] = None  # Force data refresh

st.write(f"Map will refresh in {refresh_interval} seconds.")
time.sleep(refresh_interval)
