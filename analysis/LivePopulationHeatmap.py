import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from folium.plugins import HeatMap
from google.cloud import bigquery
import time

# Google Cloud BigQuery setup
PROJECT_ID = "yteam-plutus-iisc"
TABLE_ID = "location.visited_location_view"
client = bigquery.Client.from_service_account_json('/path-to-json-key/team-plutus-iisc-a8b18655b592.json')

# Function to fetch data from BigQuery
def fetch_data(start_date, end_date):
    query = f"""
    SELECT lat_visit, lon_visit, date_visit, cluster_pred
    FROM `{TABLE_ID}`
    WHERE DATE(date_visit) BETWEEN '{start_date}' AND '{end_date}'
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

# Date range input for filtering
start_date = st.sidebar.date_input("Select Start Date", value=pd.Timestamp.now().date() - pd.Timedelta(days=7))
end_date = st.sidebar.date_input("Select End Date", value=pd.Timestamp.now().date())

# Refresh interval input
refresh_interval = st.sidebar.number_input(
    "Refresh Interval (seconds)", min_value=5, max_value=60, value=10, step=5
)

# Fetch data from BigQuery based on the date range
data = fetch_data(start_date, end_date)

# Sidebar: Statistical Insights
st.sidebar.markdown("### Key Insights")

# Total number of visits
total_visits = len(data)
st.sidebar.metric("Total Visits", total_visits)

if not data.empty:
    # Most visited location
    most_visited_location = data.groupby(["lat_visit", "lon_visit"]).size().idxmax()
    most_visited_count = data.groupby(["lat_visit", "lon_visit"]).size().max()
    st.sidebar.write(f"**Most Visited Location**: {most_visited_location} ({most_visited_count} visits)")

    # Visits by cluster
    if 'cluster_pred' in data.columns:
        cluster_counts = data['cluster_pred'].value_counts()
        st.sidebar.markdown("#### Visits by Cluster")
        for cluster, count in cluster_counts.items():
            st.sidebar.write(f"Cluster {cluster}: {count} visits")
else:
    st.sidebar.warning("No data available for insights.")

# Main Map Visualization
if data.empty:
    st.warning("No data found for the selected date range.")
else:
    # Get the mean lat/lon for centering the map
    mean_lat = data["lat_visit"].mean()
    mean_lon = data["lon_visit"].mean()

    # Create the map dynamically based on lat/lon from the data
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=14)

    # Prepare data for HeatMap (latitude, longitude)
    heat_data = data[["lat_visit", "lon_visit"]].values.tolist()

    # Add HeatMap to the map
    HeatMap(heat_data, radius=10, blur=15).add_to(m)

    # Display the map in Streamlit
    st_folium(m, width=700, height=500)

# Trigger auto-refresh using a button
if st.button("Refresh Map"):
    st.experimental_rerun()  # Rerun the Streamlit script to fetch new data

st.write(f"Map will refresh in {refresh_interval} seconds.")
time.sleep(refresh_interval)